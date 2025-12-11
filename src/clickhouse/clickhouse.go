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

package clickhouse

import (
  "container/list"
  "context"
  "crypto/tls"
  "fmt"
  "strings"
  "sync"
  "time"

  "github.com/ClickHouse/clickhouse-go/v2"
  "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

  "dnstap2clickhouse/src/log"
  "dnstap2clickhouse/src/aggregator"

)

const RETRY_MAX_INTERVAL = 300
const RETRY_INTERVAL_STEP = 10
const RETRY_MAX_ITEMS = 16

type Config struct {
  Hosts string
  Secure bool
  InsecureSkipVerify bool
  Username string
  Password string
  Database string
  QueryTable string
  QueryTimeColumn string
  ResponseTable string
  ResponseTimeColumn string
  ResponseStatusColumn string
  IdentityColumn string
  QueryAddressColumn string
  QuestionNameColumn string
  QuestionTypeColumn string
  CounterColumn string
}

type ClickHouse struct {
  Config Config
  Conn clickhouse.Conn
  ConnOptions clickhouse.Options
  ReadChannel chan *aggregator.MessageList
  QueryCounter uint
  ResponseCounter uint
}

func Init(ch *ClickHouse) (*ClickHouse) {
  ch.ConnOptions = clickhouse.Options{
    Addr: strings.Split(ch.Config.Hosts, ","),
    Auth: clickhouse.Auth{
        Database: ch.Config.Database,
        Username: ch.Config.Username,
        Password: ch.Config.Password,
    },
    Settings: clickhouse.Settings{
        "max_execution_time": 60,
    },
    Compression: &clickhouse.Compression{
        Method: clickhouse.CompressionLZ4,
    },
    DialTimeout:      time.Duration(5) * time.Second,
    MaxOpenConns:     2,
    MaxIdleConns:     2,
    ConnMaxLifetime:  time.Duration(8) * time.Hour, 
    BlockBufferSize:  10,
  } 

  if ch.Config.Secure {
    ch.ConnOptions.TLS = &tls.Config {
      InsecureSkipVerify: ch.Config.InsecureSkipVerify }
  }
  ch.ReadChannel = make(chan *aggregator.MessageList, 8)
  log.Debug.Printf("ClickHouse Client created: %s", ch.Config.Hosts)
  return ch
}

func (ch *ClickHouse) Close() {
  close(ch.ReadChannel)
  if ch.Conn != nil {
    ch.Conn.Close()
  }
  log.Info.Printf("ClickHouse closed.\n")
}

func appendColumnName(stmt *string, name *string, counter *uint) {
  if len(*name) > 0 {
    if *counter > 0 {
      *stmt += ","
    }
    *stmt += *name
    (*counter)++
  }
}
func appendColumnValue(cols *[]any, name *string, c any) {
  if len(*name) > 0 {
    *cols = append(*cols, c)
  }
}

func (ch *ClickHouse) initQueryStmt() (string, uint) {
  stmt := fmt.Sprintf("INSERT INTO %s (", ch.Config.QueryTable)
  numberCols := uint(0)
  appendColumnName(&stmt, &ch.Config.QueryTimeColumn, &numberCols)
  appendColumnName(&stmt, &ch.Config.IdentityColumn, &numberCols)
  appendColumnName(&stmt, &ch.Config.QueryAddressColumn, &numberCols)
  appendColumnName(&stmt, &ch.Config.QuestionNameColumn, &numberCols)
  appendColumnName(&stmt, &ch.Config.QuestionTypeColumn, &numberCols)
  appendColumnName(&stmt, &ch.Config.CounterColumn, &numberCols)
  stmt += ")"
  log.Debug.Printf("Insert statement %s", stmt)
  return stmt, numberCols
}
func (ch *ClickHouse) initResponseStmt() (string, uint) {
  stmt := fmt.Sprintf("INSERT INTO %s (", ch.Config.ResponseTable)
  numberCols := uint(0)
  appendColumnName(&stmt, &ch.Config.ResponseTimeColumn, &numberCols)
  appendColumnName(&stmt, &ch.Config.IdentityColumn, &numberCols)
  appendColumnName(&stmt, &ch.Config.ResponseStatusColumn, &numberCols)
  appendColumnName(&stmt, &ch.Config.QueryAddressColumn, &numberCols)
  appendColumnName(&stmt, &ch.Config.QuestionNameColumn, &numberCols)
  appendColumnName(&stmt, &ch.Config.QuestionTypeColumn, &numberCols)
  appendColumnName(&stmt, &ch.Config.CounterColumn, &numberCols)
  stmt += ")"
  log.Debug.Printf("Insert statement %s", stmt)
  return stmt, numberCols
}

func (ch *ClickHouse) writeQueryStmt(context context.Context,
                                     stmt string, numberCols uint,
                                     m *aggregator.MessageList) (uint, error) {
  var count uint = 0
  l := m.List
  if l == nil {
    return 0, nil
  }
  batch, err := ch.Conn.PrepareBatch(context, stmt,
    driver.WithReleaseConnection())
  if err != nil {
    return 0, err
  }
  for e := l.Front(); e != nil; e = e.Next() {
    q := e.Value.(*aggregator.Query)
    var columns = make([]any, 0, numberCols)
    appendColumnValue(&columns, &ch.Config.QueryTimeColumn, q.QueryTime)
    appendColumnValue(&columns, &ch.Config.IdentityColumn, q.Identity)
    appendColumnValue(&columns, &ch.Config.QueryAddressColumn, q.QueryAddress)
    appendColumnValue(&columns, &ch.Config.QuestionNameColumn, q.QuestionName)
    appendColumnValue(&columns, &ch.Config.QuestionTypeColumn, q.QuestionType)
    appendColumnValue(&columns, &ch.Config.CounterColumn, q.Counter)
    log.Debug.Printf("Batch sent size.\n", columns...)
    err = batch.Append(columns...)
    if err != nil {
      log.Error.Printf("%s.\n", err)
      continue
    }
    count++
  }
  if count > 0 {
    err = batch.Send()
    if err != nil {
      return 0, err
    }
    ch.QueryCounter++
    log.Debug.Printf("Batch sent size %d.\n", count)
  }
  return count, nil
}

func (ch *ClickHouse) writeResponseStmt(context context.Context,
                                        stmt string, numberCols uint,
                                        m *aggregator.MessageList) (uint, error) {
  var count uint = 0
  l := m.List
  if l == nil {
    return 0, nil
  }
  batch, err := ch.Conn.PrepareBatch(context, stmt,
    driver.WithReleaseConnection())
  if err != nil {
    log.Error.Printf("%s.\n", err)
    return 0, err
  }
  for e := l.Front(); e != nil; e = e.Next() {
    r := e.Value.(*aggregator.Response)
    var columns = make([]any, 0, numberCols)
    appendColumnValue(&columns, &ch.Config.ResponseTimeColumn, r.ResponseTime)
    appendColumnValue(&columns, &ch.Config.IdentityColumn, r.Identity)
    appendColumnValue(&columns, &ch.Config.ResponseStatusColumn, r.ResponseStatus)
    appendColumnValue(&columns, &ch.Config.QueryAddressColumn, r.QueryAddress)
    appendColumnValue(&columns, &ch.Config.QuestionNameColumn, r.QuestionName)
    appendColumnValue(&columns, &ch.Config.QuestionTypeColumn, r.QuestionType)
    appendColumnValue(&columns, &ch.Config.CounterColumn, r.Counter)
    log.Debug.Printf("Batch sent size.\n", columns...)
    err = batch.Append(columns...)
    if err != nil {
      log.Error.Printf("%s.\n", err)
      return 0, err
    }
    count++
  }
  if count > 0 {
    err = batch.Send()
    if err != nil {
      log.Error.Printf("%s.\n", err)
      return 0, err
    }
    ch.ResponseCounter++
    log.Debug.Printf("Batch sent size %d.\n", count)
  }
  return count, nil
}
func (ch *ClickHouse) writeMessageList(context context.Context,
                                       queryStmt string,
                                       numberQueryCols uint,
                                       responseStmt string,
                                       numberResponseCols uint,
                                       m *aggregator.MessageList) error {
  switch m.Type {
  case aggregator.QueryType:
    log.Debug.Printf("Readed query list")
    _, err := ch.writeQueryStmt(context, queryStmt, numberQueryCols, m)
    if err != nil {
      return err
    }
  case aggregator.ResponseType:
    log.Debug.Printf("Readed response list")
    _, err := ch.writeResponseStmt(context, responseStmt, numberResponseCols, m)
    if err != nil {
      return err
    }
  }
  return nil
}

func (ch *ClickHouse) Run(context context.Context, wg *sync.WaitGroup) {
  defer wg.Done()
  defer ch.Close()
  var err error  

  writeQueries := true
  writeResponses := true

  if len(ch.Config.QueryTable) == 0 {
    writeQueries = false
  }
  if len(ch.Config.ResponseTable) == 0 {
    writeResponses = false
  }

  if !writeQueries && !writeResponses {
    log.Warn.Printf("Nothing to write, check QueryTable and ResponseTable configuration")
    return
  }

  ch.Conn, err = clickhouse.Open(&ch.ConnOptions)
  if err != nil {
    log.Error.Printf("%s.\n", err)
    return;
  }
  queryStmt, numberQueryCols := ch.initQueryStmt()
  if numberQueryCols == 0 {
    log.Warn.Printf("No columns to insert queries")
  }

  responseStmt, numberResponseCols := ch.initResponseStmt()
  if numberResponseCols == 0 {
    log.Warn.Printf("No columns to insert queries")
    return
  }

  retryTimer := time.NewTimer(0)
  retryInterval := 0
  retryList := list.New()

  for {  
    select {
    case <-context.Done():
      return
    case m, ok := <-ch.ReadChannel:
      if ok == false { // chan closed
        log.Warn.Printf("Query channel closed.\n")
        return
      }
      retryLen := retryList.Len()

      if retryLen >= RETRY_MAX_ITEMS {
        // discard
        log.Debug.Printf("Retry list full")
        continue
      }
      if retryLen > 0 {
        // continue enqueing
        retryList.PushBack(m)
        continue
      }
      // retryLen = 0, process normaly

      err := ch.writeMessageList(context, queryStmt, numberQueryCols,
                                 responseStmt, numberResponseCols, m)
      if err != nil {
        log.Error.Printf("%s retrying...", err)
        retryInterval = RETRY_INTERVAL_STEP
        log.Debug.Printf("Reset retry timer to %d seconds", retryInterval)
        retryTimer.Reset(time.Duration(retryInterval) * time.Second)
        retryList.PushBack(m)
        continue
      }
    case <-retryTimer.C:
      for e := retryList.Front(); e != nil; e = retryList.Front()  {
        m := e.Value.(*aggregator.MessageList)
        err := ch.writeMessageList(context, queryStmt, numberQueryCols,
                                   responseStmt, numberResponseCols, m)
        if err != nil {
          log.Error.Printf("%s retrying...", err)
          if retryInterval < RETRY_MAX_INTERVAL {
            retryInterval += RETRY_INTERVAL_STEP
          }
          log.Warn.Printf("Reset retry timer to %d seconds", retryInterval)
          retryTimer.Reset(time.Duration(retryInterval) * time.Second)
          break
        }
        retryInterval = 0
        retryList.Remove(e)
      }
    }
  }
}
      
func (ch *ClickHouse) Stats() (uint, uint){
  queryCounter := ch.QueryCounter
  responseCounter := ch.ResponseCounter
  ch.QueryCounter = 0
  ch.ResponseCounter = 0
  return queryCounter, responseCounter
}
