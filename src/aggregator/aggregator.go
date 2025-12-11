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

type Query struct {
  QueryTime time.Time
  Identity string
  QueryAddress string
  QuestionName string
  QuestionType string
  Counter uint64
}

func (q Query) getCounter() uint64  { return q.Counter }
func (q Query) setCounter(c uint64) { q.Counter = c }

type Response struct {
  ResponseTime time.Time
  Identity string
  ResponseStatus string
  QueryAddress string
  QuestionName string
  QuestionType string
  Counter uint64
}

func (r Response) getCounter() uint64  { return r.Counter }
func (r Response) setCounter(c uint64) { r.Counter = c }

type HasCounter interface {
  getCounter() uint64
  setCounter(uint64)
}

type MessageType int
const (
  QueryType      MessageType = iota + 1 // EnumIndex = 1
  ResponseType                          // EnumIndex = 2
)

type Message struct {
  Type MessageType
  Message any
}

type MessageList struct {
  Type MessageType
  List *list.List
}

type Aggregator struct {
  Config Config
  ReadChannel chan *Message
  WriteChannel chan *MessageList
  QueryAggregationMap QueryAggregationMap
  ResponseAggregationMap ResponseAggregationMap
  QueryList *list.List
  ResponseList *list.List
  QueryMutex *sync.Mutex
  ResponseMutex *sync.Mutex
  QueryCounter uint
  ResponseCounter uint
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

type QueryMapKey struct {
  Identity string
  QueryAddress string
  QuestionName string
  QuestionType string
}
type ResponseMapKey struct {
  Identity string
  ResponseStatus string
  QueryAddress string
  QuestionName string
  QuestionType string
}

type QueryAggregationMap map[QueryMapKey]*Query
type ResponseAggregationMap map[ResponseMapKey]*Response

type MapKey interface {
  QueryMapKey | ResponseMapKey
}

func Init(a *Aggregator) (*Aggregator) {
  a.ReadChannel = make(chan *Message, 256)
  a.QueryAggregationMap = make(QueryAggregationMap)
  a.ResponseAggregationMap = make(ResponseAggregationMap)
  a.QueryMutex = &sync.Mutex{}
  a.ResponseMutex = &sync.Mutex{}

  return a
}

func (a *Aggregator) Close() {
  close(a.ReadChannel)
  clear(a.QueryAggregationMap)
  clear(a.ResponseAggregationMap)
  log.Info.Printf("Aggregator closed.\n")
}

func increaseCounter[K MapKey, V HasCounter](
                     m map[K]*V, key K, value *V) {
  v, ok := m[key]
  if !ok {
    m[key] = value
  } else {
    (*v).setCounter((*v).getCounter() + (*value).getCounter())
  }
  log.Debug.Printf("Increase Counter for %+v", key)
}

func (a *Aggregator) AggregateQuery(q *Query) {
  log.Trace.Printf("WriteUngrouped %s", a.Config.WriteUngrouped)
  log.Trace.Printf("GroupbyQuestion %s", a.Config.GroupbyQuestion)
  log.Trace.Printf("GroupbyQueryAddress %s", a.Config.GroupbyQueryAddress)

  a.QueryMutex.Lock()
  if (a.Config.WriteUngrouped) {
    increaseCounter(a.QueryAggregationMap,
                    QueryMapKey{ Identity: q.Identity,
                                 QueryAddress: q.QueryAddress,
                                 QuestionName: q.QuestionName,
                                 QuestionType: q.QuestionType },
                    q)
  }
  if (a.Config.GroupbyQuestion) {
    increaseCounter(a.QueryAggregationMap,
                    QueryMapKey{ Identity: q.Identity,
                                 QueryAddress: q.QueryAddress,
                                 QuestionName: GROUPBY_TAG,
                                 QuestionType: GROUPBY_TAG },
                    &Query{ QueryTime: q.QueryTime,
                            Identity: q.Identity,
                            QueryAddress: q.QueryAddress,
                            QuestionName: GROUPBY_TAG,
                            QuestionType: GROUPBY_TAG,
                            Counter: q.Counter })
  }
  if (a.Config.GroupbyQueryAddress) {
    increaseCounter(a.QueryAggregationMap,
                    QueryMapKey{ Identity: q.Identity,
                                 QueryAddress: GROUPBY_TAG,
                                 QuestionName: q.QuestionName,
                                 QuestionType: q.QuestionType },
                    &Query{ QueryTime: q.QueryTime,
                            Identity: q.Identity,
                            QueryAddress: GROUPBY_TAG,
                            QuestionName: q.QuestionName,
                            QuestionType: q.QuestionType,
                            Counter: q.Counter })
  }

  a.QueryMutex.Unlock()
}

func (a *Aggregator) AggregateResponse(r *Response) {
  log.Trace.Printf("WriteUngrouped %s", a.Config.WriteUngrouped)
  log.Trace.Printf("GroupbyQuestion %s", a.Config.GroupbyQuestion)
  log.Trace.Printf("GroupbyQueryAddress %s", a.Config.GroupbyQueryAddress)

  a.ResponseMutex.Lock()
  if (a.Config.WriteUngrouped) {
    increaseCounter(a.ResponseAggregationMap,
                    ResponseMapKey{ Identity: r.Identity,
                                    ResponseStatus: r.ResponseStatus,
                                    QueryAddress: r.QueryAddress,
                                    QuestionName: r.QuestionName,
                                    QuestionType: r.QuestionType },
                    r)
  }
  if (a.Config.GroupbyQuestion) {
    increaseCounter(a.ResponseAggregationMap,
                    ResponseMapKey{ Identity: r.Identity,
                                    ResponseStatus: r.ResponseStatus,
                                    QueryAddress: r.QueryAddress,
                                    QuestionName: GROUPBY_TAG,
                                    QuestionType: GROUPBY_TAG },
                    &Response{ ResponseTime: r.ResponseTime,
                               Identity: r.Identity,
                               ResponseStatus: r.ResponseStatus,
                               QueryAddress: r.QueryAddress,
                               QuestionName: GROUPBY_TAG,
                               QuestionType: GROUPBY_TAG,
                               Counter: r.Counter })
  }
  if (a.Config.GroupbyQueryAddress) {
    increaseCounter(a.ResponseAggregationMap,
                    ResponseMapKey{ Identity: r.Identity,
                                    ResponseStatus: r.ResponseStatus,
                                    QueryAddress: GROUPBY_TAG,
                                    QuestionName: r.QuestionName,
                                    QuestionType: r.QuestionType },
                    &Response{ ResponseTime: r.ResponseTime,
                               Identity: r.Identity,
                               ResponseStatus: r.ResponseStatus,
                               QueryAddress: GROUPBY_TAG,
                               QuestionName: r.QuestionName,
                               QuestionType: r.QuestionType,
                               Counter: r.Counter })
  }
  a.ResponseMutex.Unlock()
}

func (a *Aggregator) BuildQueryAggregationList() {
  a.QueryList = list.New()

  var total uint = 0
  a.QueryMutex.Lock()
  for _, query := range a.QueryAggregationMap {
    log.Debug.Printf("Aggregator query: %s %s %s %s %d\n",
                      query.Identity, query.QueryAddress, query.QuestionName,
                      query.QuestionType, query.Counter)
    a.QueryList.PushBack(query)
    total++
    log.Debug.Printf("List Push query : %s", *query)
  }
  clear(a.QueryAggregationMap)
  a.QueryMutex.Unlock()
  if total == 0 {
    a.QueryList = nil
  }
  log.Debug.Printf("Aggregation List size %d.\n", total)
  a.QueryCounter += total
}

func (a *Aggregator) BuildResponseAggregationList() {
  a.ResponseList = list.New()

  var total uint = 0
  a.ResponseMutex.Lock()
  for _, response := range a.ResponseAggregationMap {
    log.Debug.Printf("Aggregator response: %s %s %s %s %s %d\n",
      response.Identity, response.ResponseStatus, response.QueryAddress,
      response.QuestionName, response.QuestionType, response.Counter)
    a.ResponseList.PushBack(response)
    total++
    log.Debug.Printf("List Push response : %s", *response)
  }
  clear(a.ResponseAggregationMap)
  a.ResponseMutex.Unlock()
  if total == 0 {
    a.ResponseList = nil
  }
  log.Debug.Printf("Aggregation List size %d.\n", total)
  a.ResponseCounter += total
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
    case m, ok := <-a.ReadChannel:
      if !ok { // chan closed
        log.Warn.Printf("Channel closed.\n")
        return
      }
      switch m.Type {
      case QueryType:
        q := m.Message.(*Query)
        log.Debug.Printf("Readed query: %s.\n", q)
        if a.Config.Aggregate {
          a.AggregateQuery(q)
        } else {
          if a.QueryList == nil {
            log.Debug.Printf("Creating new query list")
            a.QueryList = list.New()
          }
          log.Debug.Printf("Adding new item to query list")
          a.QueryList.PushBack(q)
        }
      case ResponseType:
        r := m.Message.(*Response)
        if a.Config.Aggregate {
          a.AggregateResponse(r)
        } else {
          if a.ResponseList == nil {
            log.Debug.Printf("Creating new response list")
            a.ResponseList = list.New()
          }
          log.Debug.Printf("Adding new item to response list")
          a.ResponseList.PushBack(r)
        }
      }

    case <-timer.C:
      log.Debug.Printf("Aggregator: fired interval %s", a.Config.WriteInterval)
      timer.Reset(a.Config.WriteInterval)
      if a.Config.Aggregate {
        a.BuildQueryAggregationList()
        a.BuildResponseAggregationList()
      }
      if a.QueryList != nil {
        log.Debug.Printf("Writing list to WriteQueryChannel")
        a.WriteChannel <- &MessageList { Type: QueryType,
                                         List: a.QueryList }
        a.QueryList = nil
      }
      if a.ResponseList != nil {
        log.Debug.Printf("Writing list to WriteResponseChannel")
        a.WriteChannel <- &MessageList { Type: ResponseType,
                                         List: a.ResponseList }
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
