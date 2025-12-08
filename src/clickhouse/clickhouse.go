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
  "fmt"
  "sync"
  "time"

  "github.com/ClickHouse/clickhouse-go/v2"
  "github.com/ClickHouse/clickhouse-go/v2/lib/driver"

  "dnstap2clickhouse/src/log"
  "dnstap2clickhouse/src/aggregator"

)

type Config struct {
  Host string
  Port int
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
  QueryChannel chan *list.List
  ResponseChannel chan *list.List
  QueryCounter uint
  ResponseCounter uint
}

func Init(ch *ClickHouse) (*ClickHouse) {
  ch.ConnOptions = clickhouse.Options{
    Addr: []string{fmt.Sprintf("%s:%d", ch.Config.Host, ch.Config.Port)},
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
    BlockBufferSize: 10,
  } 
  ch.QueryChannel = make(chan *list.List, 8)
  ch.ResponseChannel = make(chan *list.List, 8)
  log.Debug.Printf("ClickHouse Client created: %s:%d.\n", ch.Config.Host, ch.Config.Port)
  return ch
}

func (ch *ClickHouse) Close() {
  close(ch.QueryChannel)
  close(ch.ResponseChannel)
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

  queryStmt := fmt.Sprintf("INSERT INTO %s (", ch.Config.QueryTable)
  numberQueryCols := uint(0)
  appendColumnName(&queryStmt, &ch.Config.QueryTimeColumn, &numberQueryCols)
  appendColumnName(&queryStmt, &ch.Config.IdentityColumn, &numberQueryCols)
  appendColumnName(&queryStmt, &ch.Config.QueryAddressColumn, &numberQueryCols)
  appendColumnName(&queryStmt, &ch.Config.QuestionNameColumn, &numberQueryCols)
  appendColumnName(&queryStmt, &ch.Config.QuestionTypeColumn, &numberQueryCols)
  appendColumnName(&queryStmt, &ch.Config.CounterColumn, &numberQueryCols)
  queryStmt += ")"
  log.Debug.Printf("Insert statement %s", queryStmt)
  if numberQueryCols == 0 {
    log.Warn.Printf("No columns to insert queries")
  }

  responseStmt := fmt.Sprintf("INSERT INTO %s (", ch.Config.ResponseTable)
  numberResponseCols := uint(0)
  appendColumnName(&responseStmt, &ch.Config.ResponseTimeColumn, &numberResponseCols)
  appendColumnName(&responseStmt, &ch.Config.IdentityColumn, &numberResponseCols)
  appendColumnName(&responseStmt, &ch.Config.ResponseStatusColumn, &numberResponseCols)
  appendColumnName(&responseStmt, &ch.Config.QueryAddressColumn, &numberResponseCols)
  appendColumnName(&responseStmt, &ch.Config.QuestionNameColumn, &numberResponseCols)
  appendColumnName(&responseStmt, &ch.Config.QuestionTypeColumn, &numberResponseCols)
  appendColumnName(&responseStmt, &ch.Config.CounterColumn, &numberResponseCols)
  responseStmt += ")"
  log.Debug.Printf("Insert statement %s", responseStmt)
  if numberResponseCols == 0 {
    log.Warn.Printf("No columns to insert queries")
    return
  }

  for {  
    select {
    case <-context.Done():
      return
    case l, ok := <-ch.QueryChannel:
      if ok == false { // chan closed
        log.Warn.Printf("Query channel closed.\n")
        return
      }
      log.Debug.Printf("Readed query list")
      if l == nil {
        continue
      }

      batch, err := ch.Conn.PrepareBatch(context, queryStmt,
        driver.WithReleaseConnection())
      if err != nil {
        log.Error.Printf("%s.\n", err)
        continue
      }
      var count uint = 0
      for e := l.Front(); e != nil; e = e.Next() {
        q := e.Value.(*aggregator.Query)
        var columns = make([]any, 0, numberQueryCols)
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
          log.Error.Printf("%s.\n", err)
          continue 
        }
        log.Debug.Printf("Batch sent size %d.\n", count)
        ch.QueryCounter++
      }
    case l, ok := <-ch.ResponseChannel:
      if ok == false { // chan closed
        log.Warn.Printf("Response channel closed.\n")
        return
      }
      log.Debug.Printf("Readed response list")
      if l == nil {
        continue
      }

      batch, err := ch.Conn.PrepareBatch(context, responseStmt,
        driver.WithReleaseConnection())
      if err != nil {
        log.Error.Printf("%s.\n", err)
        continue
      }
      var count uint = 0
      for e := l.Front(); e != nil; e = e.Next() {
        r := e.Value.(*aggregator.Response)
        var columns = make([]any, 0, numberResponseCols)
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
          continue
        }
        count++
      }
      if count > 0 {
        err = batch.Send()
        if err != nil {
          log.Error.Printf("%s.\n", err)
          continue 
        }
        log.Debug.Printf("Batch sent size %d.\n", count)
        ch.ResponseCounter++
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
