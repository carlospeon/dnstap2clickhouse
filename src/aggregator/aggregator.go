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
  QueryAggregationMap QueryIdentityMap
  ResponseAggregationMap ResponseIdentityMap
  QueryList *list.List
  ResponseList *list.List
  QueryMutex *sync.Mutex
  ResponseMutex *sync.Mutex
  QueryCounter uint
  ResponseCounter uint
}

type Query struct {
  Identity string
  QueryAddress string
  QueryTime time.Time
  QuestionName string
  QuestionType string
  Counter uint64
}

type Response struct {
  Identity string
  QueryAddress string
  ResponseTime time.Time
  QuestionName string
  QuestionType string
  ResponseStatus string
  Counter uint64
}

type Identity = string
type QueryAddress = string
type QuestionName = string
type QuestionType = string
type ResponseStatus = string
type Counter struct {
  Timestamp time.Time
  Counter uint64
}

type QueryIdentityMap = map[Identity]QueryAddressMap
type QueryAddressMap = map[QueryAddress]QuestionNameMap
type QuestionNameMap = map[QuestionName]QuestionTypeMap
type QuestionTypeMap = map[QuestionType](*Counter)

type ResponseIdentityMap = map[Identity]ResponseStatusMap
type ResponseStatusMap = map[ResponseStatus]QueryAddressMap

func Init(a *Aggregator) (*Aggregator) {
  a.ReadQueryChannel = make(chan Query, 256)
  a.ReadResponseChannel = make(chan Response, 256)
  a.QueryAggregationMap = make(QueryIdentityMap)
  a.ResponseAggregationMap = make(ResponseIdentityMap)
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

func IncreaseQueryCounter (qim QueryIdentityMap, q Query) {
  qam, ok := qim[q.Identity]
  if !ok {
    qam = make(QueryAddressMap)
    qim[q.Identity] = qam
  }
  qnm, ok := qam[q.QueryAddress]
  if !ok {
    qnm = make(QuestionNameMap)
    qam[q.QueryAddress] = qnm
  }
  qtm, ok := qnm[q.QuestionName]
  if !ok {
    qtm = make(QuestionTypeMap)
    qnm[q.QuestionName] = qtm
  }
  counter, ok := qtm[q.QuestionType]
  if !ok {
    counter = &Counter{ Timestamp: q.QueryTime, Counter: 0 }
    qtm[q.QuestionType] = counter
  }
  counter.Counter += q.Counter
  log.Debug.Printf("Increase Couter for query %s", q)
}

func IncreaseResponseCounter (rim ResponseIdentityMap, r Response) {
  rsm, ok := rim[r.Identity]
  if !ok {
    rsm = make(ResponseStatusMap)
    rim[r.Identity] = rsm
  }
  qam, ok := rsm[r.ResponseStatus]
  if !ok {
    qam = make(QueryAddressMap)
    rsm[r.ResponseStatus] = qam
  }
  qnm, ok := qam[r.QueryAddress]
  if !ok {
    qnm = make(QuestionNameMap)
    qam[r.QueryAddress] = qnm
  }
  qtm, ok := qnm[r.QuestionName]
  if !ok {
    qtm = make(QuestionTypeMap)
    qnm[r.QuestionName] = qtm
  }
  counter, ok := qtm[r.QuestionType]
  if !ok {
    counter = &Counter{ Timestamp: r.ResponseTime, Counter: 0 }
    qtm[r.QuestionType] = counter
  }
  counter.Counter += r.Counter
  log.Debug.Printf("Increase Couter for response %s", r)
}

func (a *Aggregator) AggregateQuery(q Query) {
  log.Trace.Printf("WriteUngrouped %s", a.Config.WriteUngrouped)
  log.Trace.Printf("GroupbyQuestion %s", a.Config.GroupbyQuestion)
  log.Trace.Printf("GroupbyQueryAddress %s", a.Config.GroupbyQueryAddress)

  a.QueryMutex.Lock()
  if (a.Config.WriteUngrouped) {
    IncreaseQueryCounter(a.QueryAggregationMap, q)
  }
  if (a.Config.GroupbyQuestion) {
    IncreaseQueryCounter(a.QueryAggregationMap, Query{
      Identity: q.Identity,
      QueryAddress: q.QueryAddress,
      QueryTime: q.QueryTime,
      QuestionName: GROUPBY_TAG,
      QuestionType: GROUPBY_TAG,
      Counter: q.Counter,
    })
  }
  if (a.Config.GroupbyQueryAddress) {
    IncreaseQueryCounter(a.QueryAggregationMap, Query{
      Identity: q.Identity,
      QueryAddress: GROUPBY_TAG,
      QueryTime: q.QueryTime,
      QuestionName: q.QuestionName,
      QuestionType: q.QuestionType,
      Counter: q.Counter,
    })
  }
  a.QueryMutex.Unlock()
}

func (a *Aggregator) AggregateResponse(r Response) {
  log.Trace.Printf("WriteUngrouped %s", a.Config.WriteUngrouped)
  log.Trace.Printf("GroupbyQuestion %s", a.Config.GroupbyQuestion)
  log.Trace.Printf("GroupbyQueryAddress %s", a.Config.GroupbyQueryAddress)

  a.ResponseMutex.Lock()
  if (a.Config.WriteUngrouped) {
    IncreaseResponseCounter(a.ResponseAggregationMap, r)
  }
  if (a.Config.GroupbyQuestion) {
    IncreaseResponseCounter(a.ResponseAggregationMap, Response{
      Identity: r.Identity,
      QueryAddress: r.QueryAddress,
      ResponseTime: r.ResponseTime,
      ResponseStatus: r.ResponseStatus,
      QuestionName: GROUPBY_TAG,
      QuestionType: GROUPBY_TAG,
      Counter: r.Counter,
    })
  }
  if (a.Config.GroupbyQueryAddress) {
    IncreaseResponseCounter(a.ResponseAggregationMap, Response{
      Identity: r.Identity,
      QueryAddress: GROUPBY_TAG,
      ResponseTime: r.ResponseTime,
      ResponseStatus: r.ResponseStatus,
      QuestionName: r.QuestionName,
      QuestionType: r.QuestionType,
      Counter: r.Counter,
    })
  }
  a.ResponseMutex.Unlock()
}

func (a *Aggregator) BuildQueryAggregationList() {
  a.QueryList = list.New()

  var total uint = 0
  a.QueryMutex.Lock()
  for identity, queryAddressMap := range a.QueryAggregationMap {
    for queryAddress, questionNameMap := range queryAddressMap {
      for questionName, questionTypeMap := range questionNameMap {
        for questionType, counter := range questionTypeMap {
          log.Debug.Printf("Aggregator query: %s %s %s %s %d\n", 
                            identity, queryAddress, questionName, questionType, counter)
          query := Query{
            Identity: identity,
            QueryAddress: queryAddress,
            QueryTime: counter.Timestamp,
            QuestionName: questionName,
            QuestionType: questionType,
            Counter: counter.Counter,
          }
          a.QueryList.PushBack(&query)
          total++
          log.Debug.Printf("List Push query : %s", query)
        }
        clear(questionTypeMap)
      }
      clear(questionNameMap)
    }
    clear(queryAddressMap)
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
    for responseStatus, queryAddressMap := range responseStatusMap {
      for queryAddress, questionNameMap := range queryAddressMap {
        for questionName, questionTypeMap := range questionNameMap {
          for questionType, counter := range questionTypeMap {
            log.Debug.Printf("Aggregator response: %s %s %s %s %s %d\n", 
              identity, responseStatus, queryAddress, questionName, questionType, counter)
            response := Response{
              Identity: identity,
              QueryAddress: queryAddress,
              ResponseTime: counter.Timestamp,
              ResponseStatus: responseStatus,
              QuestionName: questionName,
              QuestionType: questionType,
              Counter: counter.Counter,
            }
            a.ResponseList.PushBack(&response)
            total++
            log.Debug.Printf("List Push response : %s", response)
          }
          clear(questionTypeMap)
        }
        clear(questionNameMap)
      }
      clear(queryAddressMap)
    }
    clear(responseStatusMap)
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
      if ok == false { // chan closed
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
      if ok == false { // chan closed
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
