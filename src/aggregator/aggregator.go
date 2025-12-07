/*
 * Copyright (C) 2025 Carlos Peón Costa
 *
 * This file is part of dnstap2clickhouse.
 *
 * dnstap2clickhouse is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by the
 * Free Software Foundation, either version 3 of the License, or (at your 
 * option) any later version.
 *
 * dnstap2clickhouse is distributed in the hope that it will be useful, but
 * WITHOUT ANY WARRANTY; without even the implied warranty of MERCHANTABILITY
 * or FITNESS FOR A PARTICULAR PURPOSE. See the GNU General Public License
 * for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with dnstap2clickhouse. If not, see <https://www.gnu.org/licenses/>.
*/

package aggregator

import (
  "container/list"
  "context"
  "time"
  "sync"

  "dnstap2clickhouse/src/log"
)

const GROUPBY_TAG = "__ANY__"

type Config struct {
  WriteInterval time.Duration
  Aggregate bool
  WriteUngrouped bool
  GroupbyQueryAddress bool
  GroupbyQuestion bool
}
type Aggregator struct {
  Config Config
  ReadQueryChannel chan Query
  ReadResponseChannel chan Response
  WriteQueryChannel chan *list.List
  WriteResponseChannel chan *list.List
  QueryAggregationMap AggregationMap
  ResponseAggregationMap AggregationMap
  QueryList *list.List
  ResponseList *list.List
  QueryMutex *sync.Mutex
  ResponseMutex *sync.Mutex
  QueryCounter uint
  ResponseCounter uint
}

type Query struct {
  QueryTime time.Time
  Identity string
  QueryAddress string
  QuestionName string
  QuestionType string
  Counter uint64
}

type Response struct {
  ResponseTime time.Time
  Identity string
  ResponseStatus string
  QueryAddress string
  QuestionName string
  QuestionType string
  Counter uint64
}

type Identity = string
type QueryAddress = string
type QuestionName = string
type QuestionNamePart = string
type QuestionType = string
type ResponseStatus = string
type Counter struct {
  Timestamp time.Time
  Counter uint64
}

type AggregationMap = map[string]any

type QuestionNamePartMap struct {
	Last bool
	Map map[QuestionNamePart]any
}

func Init(a *Aggregator) (*Aggregator) {
  a.ReadQueryChannel = make(chan Query, 256)
  a.ReadResponseChannel = make(chan Response, 256)
	a.QueryAggregationMap = make(AggregationMap)
  a.ResponseAggregationMap = make(AggregationMap)
  a.QueryMutex = &sync.Mutex{}
  a.ResponseMutex = &sync.Mutex{}

  return a
}

func (a *Aggregator) Close() {
  close(a.ReadQueryChannel)
  close(a.ReadResponseChannel)
  clear(a.QueryAggregationMap)
  clear(a.ResponseAggregationMap)
  log.Info.Printf("Aggregator closed.\n")
}

func GetOrCreateInnerMap(rootMap AggregationMap, keys ...string) AggregationMap {
	m := rootMap
	for _, k := range keys {
		inner, ok := m[k]
		if !ok {
			inner = make(AggregationMap)
			m[k] = inner
		}
		m = inner.(AggregationMap)
	}
	return m
}

func IncreaseQueryCounter (m AggregationMap, c *Counter, path ...string) {
	
	lastMap := GetOrCreateInnerMap(m, path[:len(path) - 1]...)

	lastKey := path[len(path) - 1]
  counter, ok := lastMap[lastKey].(*Counter)
  if !ok {
    lastMap[lastKey] = c
  } else {
  	counter.Counter += c.Counter
	}
  log.Debug.Printf("Increase Counter for query %+v", path)
}

func (a *Aggregator) AggregateQuery(q Query) {
  log.Trace.Printf("WriteUngrouped %s", a.Config.WriteUngrouped)
  log.Trace.Printf("GroupbyQuestion %s", a.Config.GroupbyQuestion)
  log.Trace.Printf("GroupbyQueryAddress %s", a.Config.GroupbyQueryAddress)

  a.QueryMutex.Lock()
  if (a.Config.WriteUngrouped) {
    IncreaseQueryCounter(a.QueryAggregationMap, 
		                     &Counter{ Timestamp: q.QueryTime, Counter: q.Counter },
												 q.Identity, q.QueryAddress, q.QuestionName, q.QuestionType)
  }
  if (a.Config.GroupbyQuestion) {
    IncreaseQueryCounter(a.QueryAggregationMap, 
		                     &Counter{ Timestamp: q.QueryTime, Counter: q.Counter },
												 q.Identity, q.QueryAddress, GROUPBY_TAG, GROUPBY_TAG)
  }
  if (a.Config.GroupbyQueryAddress) {
    IncreaseQueryCounter(a.QueryAggregationMap, 
		                     &Counter{ Timestamp: q.QueryTime, Counter: q.Counter },
												 q.Identity, GROUPBY_TAG, q.QuestionName, q.QuestionType)
  }

  a.QueryMutex.Unlock()
}

func (a *Aggregator) AggregateResponse(r Response) {
  log.Trace.Printf("WriteUngrouped %s", a.Config.WriteUngrouped)
  log.Trace.Printf("GroupbyQuestion %s", a.Config.GroupbyQuestion)
  log.Trace.Printf("GroupbyQueryAddress %s", a.Config.GroupbyQueryAddress)

  a.ResponseMutex.Lock()
  if (a.Config.WriteUngrouped) {
    IncreaseQueryCounter(a.ResponseAggregationMap, 
		                     &Counter{ Timestamp: r.ResponseTime, Counter: r.Counter },
												 r.Identity, r.ResponseStatus, r.QueryAddress, r.QuestionName, r.QuestionType)
  }
  if (a.Config.GroupbyQuestion) {
    IncreaseQueryCounter(a.ResponseAggregationMap, 
		                     &Counter{ Timestamp: r.ResponseTime, Counter: r.Counter },
												 r.Identity, r.ResponseStatus, r.QueryAddress, GROUPBY_TAG, GROUPBY_TAG)
  }
  if (a.Config.GroupbyQueryAddress) {
    IncreaseQueryCounter(a.ResponseAggregationMap, 
		                     &Counter{ Timestamp: r.ResponseTime, Counter: r.Counter },
												 r.Identity, r.ResponseStatus, GROUPBY_TAG, r.QuestionName, r.QuestionType)
  }
  a.ResponseMutex.Unlock()
}

func (a *Aggregator) BuildQueryAggregationList() {
  a.QueryList = list.New()

  var total uint = 0
  a.QueryMutex.Lock()
  for identity, queryAddressMap := range a.QueryAggregationMap {
    for queryAddress, questionNameMap := range queryAddressMap.(AggregationMap) {
      for questionName, questionTypeMap := range questionNameMap.(AggregationMap) {
        for questionType, c := range questionTypeMap.(AggregationMap) {
					counter := c.(*Counter)
          log.Debug.Printf("Aggregator query: %s %s %s %s %d\n", 
                            identity, queryAddress, questionName, questionType, counter)
          query := Query{
            QueryTime: counter.Timestamp,
            Identity: identity,
            QueryAddress: queryAddress,
            QuestionName: questionName,
            QuestionType: questionType,
            Counter: counter.Counter,
          }
          a.QueryList.PushBack(&query)
          total++
          log.Debug.Printf("List Push query : %s", query)
        }
        clear(questionTypeMap.(AggregationMap))
      }
      clear(questionNameMap.(AggregationMap))
    }
    clear(queryAddressMap.(AggregationMap))
  }
  clear(a.QueryAggregationMap)
  a.QueryMutex.Unlock()
  if total == 0 {
    a.QueryList = nil
  }
  log.Debug.Printf("Aggregation List size %d.\n", total)
}

func (a *Aggregator) BuildResponseAggregationList() {
  a.ResponseList = list.New()

  var total uint = 0
  a.ResponseMutex.Lock()
  for identity, responseStatusMap := range a.ResponseAggregationMap {
    for responseStatus, queryAddressMap := range responseStatusMap.(AggregationMap) {
      for queryAddress, questionNameMap := range queryAddressMap.(AggregationMap) {
        for questionName, questionTypeMap := range questionNameMap.(AggregationMap) {
          for questionType, c := range questionTypeMap.(AggregationMap) {
					  counter := c.(*Counter)
            log.Debug.Printf("Aggregator response: %s %s %s %s %s %d\n", 
              identity, responseStatus, queryAddress, questionName, questionType, counter)
            response := Response{
              ResponseTime: counter.Timestamp,
              Identity: identity,
              ResponseStatus: responseStatus,
              QueryAddress: queryAddress,
              QuestionName: questionName,
              QuestionType: questionType,
              Counter: counter.Counter,
            }
            a.ResponseList.PushBack(&response)
            total++
            log.Debug.Printf("List Push response : %s", response)
          }
          clear(questionTypeMap.(AggregationMap))
        }
        clear(questionNameMap.(AggregationMap))
      }
      clear(queryAddressMap.(AggregationMap))
    }
    clear(responseStatusMap.(AggregationMap))
  }
  clear(a.ResponseAggregationMap)
  a.ResponseMutex.Unlock()
  if total == 0 {
    a.ResponseList = nil
  }
  log.Debug.Printf("Aggregation List size %d.\n", total)
}

func (a *Aggregator) Run (context context.Context, wg *sync.WaitGroup) {
  defer wg.Done()
  defer a.Close()
  log.Debug.Printf("Running aggregator with interval %s\n", a.Config.WriteInterval)

  timer := time.NewTimer(a.Config.WriteInterval)
  for {
    select {
    case <-context.Done():
      return
    case q, ok := <-a.ReadQueryChannel:
      if !ok { // chan closed
        log.Warn.Printf("Query Channel closed.\n")
        return
      }
      log.Debug.Printf("Readed query: %s.\n", q)
      if a.Config.Aggregate {
        a.AggregateQuery(q)
      } else {
        if a.QueryList == nil {
          log.Debug.Printf("Creating new query list")
          a.QueryList = list.New()
        }
        log.Debug.Printf("Adding new item to query list")
        a.QueryList.PushBack(&q)
      }
      a.QueryCounter++
    case r, ok := <-a.ReadResponseChannel:
      if !ok { // chan closed
        log.Warn.Printf("Response Channel closed.\n")
        return
      }
      log.Debug.Printf("Readed response: %s.\n", r)
      if a.Config.Aggregate {
        a.AggregateResponse(r)
      } else {
        if a.ResponseList == nil {
          log.Debug.Printf("Creating new response list")
          a.ResponseList = list.New()
        }
        log.Debug.Printf("Adding new item to response list")
        a.ResponseList.PushBack(&r)
      }
      a.ResponseCounter++

    case <-timer.C:
      log.Debug.Printf("Aggregator: fired interval %s", a.Config.WriteInterval)
      timer.Reset(a.Config.WriteInterval)
      if a.Config.Aggregate {
        a.BuildQueryAggregationList()
        a.BuildResponseAggregationList()
      }
      if a.QueryList != nil {
        log.Debug.Printf("Writing list to WriteQueryChannel")
        a.WriteQueryChannel <- a.QueryList
        a.QueryList = nil
      }
      if a.ResponseList != nil {
        log.Debug.Printf("Writing list to WriteResponseChannel")
        a.WriteResponseChannel <- a.ResponseList
        a.ResponseList = nil
      }
    }
  }
}

func (a *Aggregator) Stats() (uint, uint){
  queryCounter := a.QueryCounter
  responseCounter := a.ResponseCounter
  a.QueryCounter = 0
  a.ResponseCounter = 0
  return queryCounter, responseCounter
}
