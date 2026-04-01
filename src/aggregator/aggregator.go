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
/*
 * In intervals of WriteInterval / 2 sample query/responses and match them to
 * get the response time delta
 */
const MAX_QUERY_RESPONSE_MAP_SIZE = 8
const MAX_QUERY_RESPONSE_MAP_SIZE_EXCEEDS = MAX_QUERY_RESPONSE_MAP_SIZE / 10 + 1
const MIN_QUERY_RESPONSE_SAMPLES = 32
const MAX_QUERY_RESPONSE_SAMPLES = 256

type Config struct {
  WriteInterval time.Duration
  Aggregate bool
  WriteUngrouped bool
  GroupbyQueryAddress bool
  GroupbyQuestion bool
  ClientQueries bool
  NonOkClientResponses bool
	ClientResponseTimeSamples bool
}

type DnsIdType = uint16
type Query struct {
  QueryTime time.Time
  Identity string
  QueryAddress string
  QueryPort uint32
  QuestionName string
  QuestionType string
  Id DnsIdType
  Counter uint64
}

func (q *Query) isResponse() bool  { return false }
func (q *Query) getIdentity() string  { return q.Identity }
func (q *Query) getQueryAddress() string  { return q.QueryAddress }
func (q *Query) getQueryPort() uint32  { return q.QueryPort }
func (q *Query) getId() DnsIdType  { return q.Id }
func (q *Query) getTime() time.Time  { return q.QueryTime }
func (q *Query) getCounter() uint64  { return q.Counter }
func (q *Query) setCounter(c uint64) { q.Counter = c }

type Response struct {
  ResponseTime time.Time
  Identity string
  ResponseStatus string
  QueryAddress string
  QueryPort uint32
  QuestionName string
  QuestionType string
  Id DnsIdType
	IsSuccess bool
  Counter uint64
}

func (r *Response) isResponse() bool  { return true }
func (r *Response) getIdentity() string  { return r.Identity }
func (r *Response) getQueryAddress() string  { return r.QueryAddress }
func (r *Response) getQueryPort() uint32  { return r.QueryPort }
func (r *Response) getId() DnsIdType  { return r.Id }
func (r *Response) getTime() time.Time  { return r.ResponseTime }
func (r *Response) getCounter() uint64  { return r.Counter }
func (r *Response) setCounter(c uint64) { r.Counter = c }

type HasClientResponseTime interface {
	isResponse() bool
	getIdentity() string
	getQueryAddress() string
	getQueryPort() uint32
	getId() DnsIdType
	getTime() time.Time
}

type HasCounter interface {
  getCounter() uint64
  setCounter(uint64)
}

type MessageType int
const (
  QueryType      MessageType = iota + 1 // EnumIndex = 1
  ResponseType                          // EnumIndex = 2
  ResponseTimeSampleType                // EnumIndex = 3
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
  QueryResponseTimeSampleMap QueryResponseTimeSampleMap
  ResponseTimeSampleMap ResponseTimeSampleMap
  QueryList *list.List
  ResponseList *list.List
  ResponseTimeSampleList *list.List
  QueryMutex *sync.Mutex
  ResponseMutex *sync.Mutex
  ResponseTimeSampleMutex *sync.Mutex
  QueryCounter uint
  ResponseCounter uint
  ResponseTimeSampleCounter uint
	QueryResponseTimeSampleMask DnsIdType
	QueryResponseTimeSampleMapSizeExceeds uint32
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
type QueryResponseTimeSampleMapKey struct {
  Identity string
  QueryAddress string
  QueryPort uint32
  Id DnsIdType
}
type ResponseTimeSample struct {
	ResponseTime time.Time
  Identity string
  ResponseTimeMicroSec uint64
  Counter uint64
}

type QueryAggregationMap map[QueryMapKey]*Query
type ResponseAggregationMap map[ResponseMapKey]*Response

type MapKey interface {
  QueryMapKey | ResponseMapKey
}

type QueryResponseTimeSampleMap map[QueryResponseTimeSampleMapKey]time.Time
type ResponseTimeSampleMap map[string]*ResponseTimeSample

func Init(a *Aggregator) (*Aggregator) {
  a.ReadChannel = make(chan *Message, 256)
  a.QueryAggregationMap = make(QueryAggregationMap)
  a.ResponseAggregationMap = make(ResponseAggregationMap)
  a.QueryResponseTimeSampleMap = make(QueryResponseTimeSampleMap)
  a.ResponseTimeSampleMap = make(ResponseTimeSampleMap)
  a.QueryMutex = &sync.Mutex{}
  a.ResponseMutex = &sync.Mutex{}
  a.ResponseTimeSampleMutex = &sync.Mutex{}
	a.QueryResponseTimeSampleMask = 0
	a.QueryResponseTimeSampleMapSizeExceeds = 0

  return a
}

func (a *Aggregator) Close() {
  close(a.ReadChannel)
  clear(a.QueryAggregationMap)
  clear(a.ResponseAggregationMap)
  clear(a.QueryResponseTimeSampleMap)
  clear(a.ResponseTimeSampleMap)
  log.Info.Printf("Aggregator closed.\n")
}

func increaseCounter[K MapKey, V HasCounter](
                     m map[K]V, key K, value V) {
  v, ok := m[key]
  if !ok {
    m[key] = value
  } else {
    v.setCounter(v.getCounter() + value.getCounter())
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

func isSample[QR HasClientResponseTime](a *Aggregator, qr QR) bool {
	if a.QueryResponseTimeSampleMask == 0 {
		return true
	}
  return (qr.getId() & a.QueryResponseTimeSampleMask == 0)
}

func AggregateResponseTimeSample[QR HasClientResponseTime](a *Aggregator, qr QR) {
	if !isSample(a, qr) {
		return
	}

  a.ResponseTimeSampleMutex.Lock()
  defer a.ResponseTimeSampleMutex.Unlock()

	log.Trace.Printf("QueryResponseTimeSample isResponse: %s", qr.isResponse())

  key := QueryResponseTimeSampleMapKey {
    Identity: qr.getIdentity(),
    QueryAddress: qr.getQueryAddress(),
    QueryPort: qr.getQueryPort(),
    Id: qr.getId() }
  firstQrTime, ok := a.QueryResponseTimeSampleMap[ key ]
  if !ok {
	  mapLen := len(a.QueryResponseTimeSampleMap)
	  log.Trace.Printf("QueryResponseSample map len: %d", mapLen)
	  if mapLen > MAX_QUERY_RESPONSE_MAP_SIZE {
			a.QueryResponseTimeSampleMapSizeExceeds++
			if a.QueryResponseTimeSampleMapSizeExceeds == 0 {
				a.QueryResponseTimeSampleMapSizeExceeds--
			}
		  return
	  }
    a.QueryResponseTimeSampleMap[ key ] = qr.getTime()
		log.Trace.Printf("QueryResponseTimeSample not found, inserting time: %s", 
		  qr.isResponse(), key)

    return
  }
	var responseTimeSampleMicroSecI int64
	var queryTime time.Time
	var responseTime time.Time
	if qr.isResponse() {
		queryTime = firstQrTime
		responseTime = qr.getTime()
	} else {
		queryTime = qr.getTime()
		responseTime = firstQrTime
  }
  responseTimeSampleMicroSecI = responseTime.Sub(queryTime).Microseconds()
	if responseTimeSampleMicroSecI < 0 {
		log.Warn.Printf("ResponseTime < 0")
		return
	}
  delete(a.QueryResponseTimeSampleMap, key)
	responseTimeSampleMicroSec := uint64(responseTimeSampleMicroSecI)
	if log.IsTrace() {
		log.Trace.Printf("QueryResponseSample response time: %d, map len: %d", 
      responseTimeSampleMicroSec, len(a.QueryResponseTimeSampleMap))
	}

  responseTimeSample, ok := a.ResponseTimeSampleMap[qr.getIdentity()]
  if !ok {
    responseTimeSample = &ResponseTimeSample { 
			ResponseTime: responseTime, 
      Identity: qr.getIdentity(), 
      ResponseTimeMicroSec: responseTimeSampleMicroSec,
      Counter: 1,
    }
    a.ResponseTimeSampleMap[qr.getIdentity()] = responseTimeSample
    return
  }
  responseTimeSample.ResponseTimeMicroSec += responseTimeSampleMicroSec
  responseTimeSample.Counter++
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

func (a *Aggregator) tuneMask(samples uint64) {
	maskLog := "keep"
	if a.QueryResponseTimeSampleMapSizeExceeds > MAX_QUERY_RESPONSE_MAP_SIZE_EXCEEDS ||
	   samples > MAX_QUERY_RESPONSE_SAMPLES {
		checkOverflow := a.QueryResponseTimeSampleMask + 1
		if checkOverflow == 0 {
      maskLog = "overflow"
		} else {
			// not overflow
			a.QueryResponseTimeSampleMask = checkOverflow * 2 - 1
      maskLog = "larger"
		}
	} else if a.QueryResponseTimeSampleMapSizeExceeds == 0 && a.QueryResponseTimeSampleMask != 0 &&
	          samples < MIN_QUERY_RESPONSE_SAMPLES {
		if a.QueryResponseTimeSampleMask == 1 {
			a.QueryResponseTimeSampleMask = 0
		} else {
	    checkOverflow := a.QueryResponseTimeSampleMask + 1
		  a.QueryResponseTimeSampleMask = checkOverflow / 2 - 1
		}
    maskLog = "shorter"
	}
  log.Debug.Printf("QueryResponseTimeSampleMapSize samples %d, exceeded %d times, %s mask %b", 
	  samples, a.QueryResponseTimeSampleMapSizeExceeds, maskLog, a.QueryResponseTimeSampleMask)
	a.QueryResponseTimeSampleMapSizeExceeds = 0
}

func (a *Aggregator) BuildResponseTimeSampleAggregationList() {

  var total uint = 0
	var samples uint64 = 0
	if a.ResponseTimeSampleList == nil {
    a.ResponseTimeSampleList = list.New()
	}
  a.ResponseTimeSampleMutex.Lock()
  for _, rts := range a.ResponseTimeSampleMap {
		responseTimeMicroSecAvg := rts.ResponseTimeMicroSec / rts.Counter
		samples += rts.Counter
		log.Debug.Printf("Aggregator ResponseTimeSample: %s %d/%d: %d\n",
                      rts.Identity, rts.ResponseTimeMicroSec, rts.Counter,
										  responseTimeMicroSecAvg)
		rts.ResponseTimeMicroSec = responseTimeMicroSecAvg
    rts.Counter = 1
    a.ResponseTimeSampleList.PushBack(rts)
    total++
    log.Trace.Printf("List push Aggregator ResponseTimeSample average: %s\n",
                      *rts)
  }
  clear(a.ResponseTimeSampleMap)
  
  now := time.Now()
  for qrtsk, t := range a.QueryResponseTimeSampleMap {
    if t.Add(a.Config.WriteInterval).Before(now) {
      delete(a.QueryResponseTimeSampleMap, qrtsk)
    }
  }
  if log.IsTrace() {
    log.Trace.Printf("QueryResponseTimeSample map size %d.\n", len(a.QueryResponseTimeSampleMap))
  }
  a.ResponseTimeSampleMutex.Unlock()
  if total == 0 {
    a.ResponseTimeSampleList = nil
  }
  log.Debug.Printf("QueryResponseTimeSample List size %d.\n", total)
  a.ResponseTimeSampleCounter += total

	a.tuneMask(samples)
}

func (a *Aggregator) Run (context context.Context, wg *sync.WaitGroup) {
  defer wg.Done()
  defer a.Close()
  log.Debug.Printf("Running aggregator with interval %s\n", a.Config.WriteInterval)

  timer := time.NewTimer(a.Config.WriteInterval)
	samplesInterval := a.Config.WriteInterval / 2
  samplesTimer := time.NewTimer(samplesInterval)
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
				if a.Config.ClientResponseTimeSamples {
          AggregateResponseTimeSample(a, q)
				}
				if !a.Config.ClientQueries {
					break
				}
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
				if a.Config.ClientResponseTimeSamples {
          AggregateResponseTimeSample(a, r)
				}
				if !a.Config.NonOkClientResponses {
					break
				}
				if r.IsSuccess {
					break
				}
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

    case <-samplesTimer.C:
      log.Debug.Printf("Aggregator: fired samples interval %s", samplesInterval)
      samplesTimer.Reset(samplesInterval)
      if a.Config.ClientResponseTimeSamples {
        a.BuildResponseTimeSampleAggregationList()
      }
    case <-timer.C:
      log.Debug.Printf("Aggregator: fired interval %s", a.Config.WriteInterval)
      timer.Reset(a.Config.WriteInterval)
      if a.Config.Aggregate {
        a.BuildQueryAggregationList()
        a.BuildResponseAggregationList()
      }
      if a.QueryList != nil {
        log.Debug.Printf("Writing query list to WriteChannel")
        a.WriteChannel <- &MessageList { Type: QueryType,
                                         List: a.QueryList }
        a.QueryList = nil
      }
      if a.ResponseList != nil {
        log.Debug.Printf("Writing response list to WriteChannel")
        a.WriteChannel <- &MessageList { Type: ResponseType,
                                         List: a.ResponseList }
        a.ResponseList = nil
      }
      if a.ResponseTimeSampleList != nil {
        log.Debug.Printf("Writing response time list to WriteResponseChannel")
        a.WriteChannel <- &MessageList { Type: ResponseTimeSampleType,
                                         List: a.ResponseTimeSampleList }
        a.ResponseTimeSampleList = nil
      }
    }
  }
}

func (a *Aggregator) Stats() (uint, uint, uint){
  queryCounter := a.QueryCounter
  responseCounter := a.ResponseCounter
  responseTimeSampleCounter := a.ResponseTimeSampleCounter
  a.QueryCounter = 0
  a.ResponseCounter = 0
  a.ResponseTimeSampleCounter = 0
  return queryCounter, responseCounter, responseTimeSampleCounter
}
