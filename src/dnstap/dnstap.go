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

package dnstap

import (
  "context"
  "errors"
  "net"
  "os"
  "sync"
  "sync/atomic"
  "time"

  "dnstap2clickhouse/src/aggregator"
  "dnstap2clickhouse/src/log"

  go_dnstap "github.com/dnstap/golang-dnstap"
  "github.com/miekg/dns"
)

const MAX_READERS = 32
const MAX_UNIXSOCKETDIR_WAIT_INTERVAL = 60

type Config struct {
  UnixSocket string 
  ReadTimeout time.Duration
  Readers int
  ClientQueries bool
  NonOkClientResponses bool
}

type Dnstap struct {
  Config Config
  WriteQueryChannel chan aggregator.Query
  WriteResponseChannel chan aggregator.Response
  Listener net.Listener
  ReadersConns [MAX_READERS]*net.Conn
  ReadersNumber atomic.Uint32
  ConnChannel chan *net.Conn
  QueryCounter uint
  ResponseCounter uint
}

func Init(d *Dnstap) (*Dnstap, error) {
  if !d.Config.ClientQueries &&
     !d.Config.NonOkClientResponses {
    return nil, errors.New("Nothing to do, check configuration options " +
                            "ClientQueries and/or NonOkClientResponses")
  }
  d.ConnChannel = make(chan *net.Conn, 1)
  log.Debug.Printf("Dnstap init: %s", d)
  return d, nil
}

func (d *Dnstap) Close() {
  d.CloseListen()
  d.CloseRead()
}

func (d *Dnstap) CloseListen() {
  if d.Listener != nil {
    d.Listener.Close()
  }
  close(d.ConnChannel)

  log.Info.Printf("Dnstap Listen closed")
}
func (d *Dnstap) Listen (context context.Context,
                         cancel context.CancelFunc,
                         wg *sync.WaitGroup) {
  defer wg.Done()
  /*
  defer d.CloseListen()
  Listener.Accept locks waiting for a connection
  CloseListen() must be call in other rutine to unlock
  */
  var err error
  wait := 0
  timer := time.NewTimer(time.Duration(wait))
LOOP:
  for {
    select {
    case <-context.Done():
      return
    case <-timer.C:
      d.Listener, err = net.Listen("unix", d.Config.UnixSocket)
      if err == nil {
        break LOOP
      }
      if errors.Is(err, os.ErrNotExist) {
        /*
        Retry later, directory may be initialized
        later during dns service startup
        */
        if wait < MAX_UNIXSOCKETDIR_WAIT_INTERVAL {
          log.Warn.Printf("%s", err)
          wait = wait * 2 + 1
          timer.Reset(time.Duration(wait) * time.Second)
          log.Warn.Printf("Retring in %d seconds...", wait)
          continue
        }
      }
      log.Error.Printf("%s", err)
      cancel()
    }
  }

  err = os.Chmod(d.Config.UnixSocket, 0o660)
  if err != nil {
    log.Warn.Printf("Chmod socket: %s", err)
  }

  for {
    select {
    case <-context.Done():
      return
    default:
      conn, err := d.Listener.Accept()
      if err != nil {
        if errors.Is(err, net.ErrClosed) {
          // Listener closed, problably from d.CloseListen()
          log.Info.Printf("%s", err)
          return
        }
        log.Error.Printf("%s", err)
        continue
      }
      d.ConnChannel <- &conn
    }
  }
}

func (d *Dnstap) CloseRead() {
  for i := uint32(0); i < d.ReadersNumber.Load(); i++ {
    if d.ReadersConns[i] != nil {
      (*d.ReadersConns[i]).Close()
    }
  }
  log.Info.Printf("Dnstap Read closed")
}

func (d *Dnstap) Read(context context.Context, wg *sync.WaitGroup) {
  defer wg.Done()
  /*
  defer d.CloseRead()
  Dedode locks reading stream
  CloseRead() must be call in other rutine to unlock
  */
  var (
    ok bool
  )
  
  id := d.ReadersNumber.Add(1) - 1
  if id == MAX_READERS {
    log.Warn.Printf("Reached max number of readers %d", MAX_READERS)
    return
  }

  for {
    select {
    case <-context.Done():
      if d.ReadersConns[id] != nil {
        (*d.ReadersConns[id]).Close()
      }
      return
    case d.ReadersConns[id], ok = <- d.ConnChannel:
      if !ok { // chan closed
        return
      }
      reader, err := go_dnstap.NewReader(
        *d.ReadersConns[id], 
        &go_dnstap.ReaderOptions{
          Bidirectional: true, 
          Timeout: d.Config.ReadTimeout,
        })

      if err != nil {
        log.Warn.Printf("%s", err)
        (*d.ReadersConns[id]).Close()
        continue
      }

      decoder := go_dnstap.NewDecoder(reader, 1<<20)
      d.Decode(context, decoder)
      (*d.ReadersConns[id]).Close()
    }
  }
}

func (d *Dnstap) Decode(context context.Context,
                        dec *go_dnstap.Decoder) {
  var dt go_dnstap.Dnstap
  for {
    select {
    case <-context.Done():
      log.Warn.Printf("Decode done")
      return
    default:
    }
    err := dec.Decode(&dt)
    if err != nil {
      if errors.Is(err, net.ErrClosed) || 
         errors.Is(err, os.ErrClosed) || 
         err.Error() == "EOF" {
        break
      }
      log.Error.Printf("%s", err)
      break
    }
    if log.IsTrace() {
      json, ok := go_dnstap.JSONFormat(&dt)
      if !ok {
        log.Error.Printf("%s", err)
        continue
      }
      log.Debug.Printf("%s", json)
    }
    if *(dt.Type) != go_dnstap.Dnstap_MESSAGE {
      continue
    } 
    if dt.Message == nil {
      log.Warn.Printf("Dnstap message empty")
      continue
    }
    msg := dt.Message

    /*
    if *(msg.Type) != go_dnstap.Message_CLIENT_QUERY {
      continue
    }
    */
    switch *(msg.Type) {
    case go_dnstap.Message_CLIENT_QUERY:
      if !d.Config.ClientQueries {
        continue
      }
      if msg.QueryMessage == nil {
        log.Warn.Printf("CLIENT_QUERY without QueryMessage")
        continue
      }
      var queryTime time.Time
      if msg.QueryTimeSec != nil && msg.QueryTimeNsec != nil {
        queryTime = time.Unix(
          int64(*msg.QueryTimeSec), 
          int64(*msg.QueryTimeNsec),
        )
      } else {
        queryTime = time.Now()
      }
      dnsMsg := new(dns.Msg)
      err = dnsMsg.Unpack(msg.QueryMessage)
      if err != nil {
        log.Error.Printf("%s", err)
        continue
      }
      log.Debug.Printf("Dnstap query message: %s", dnsMsg)

      for _, question := range dnsMsg.Question {
        q := aggregator.Query{
          Identity: string(dt.Identity),
          QueryAddress: net.IP(msg.QueryAddress).String(),
          QueryTime: queryTime,
          QuestionName: question.Name,
          QuestionType: dns.Type(question.Qtype).String(),
          Counter: 1,
        }

        log.Debug.Printf("%s", q)
        d.WriteQueryChannel <- q
        d.QueryCounter++
        log.Debug.Printf("Dnstap write query: %s", q)
      }
    case go_dnstap.Message_CLIENT_RESPONSE:
      if !d.Config.NonOkClientResponses {
        continue
      }
      if msg.ResponseMessage == nil {
        log.Warn.Printf("CLIENT_RESPONSE without ResponseMessage")
        continue
      }
      var responseTime time.Time
      if msg.ResponseTimeSec != nil && msg.ResponseTimeNsec != nil {
        responseTime = time.Unix(
          int64(*msg.ResponseTimeSec), 
          int64(*msg.ResponseTimeNsec),
        )
      } else {
        responseTime = time.Now()
      }
      dnsMsg := new(dns.Msg)
      err = dnsMsg.Unpack(msg.ResponseMessage)
      if err != nil {
        log.Error.Printf("%s", err)
        continue
      }
      log.Debug.Printf("Dnstap response message: %s", dnsMsg)
      if dnsMsg.Rcode == dns.RcodeSuccess {
        // Skip no error messages
        continue
      }
      responseStatus := dns.RcodeToString[dnsMsg.Rcode]

      for _, question := range dnsMsg.Question {
        r := aggregator.Response{
          Identity: string(dt.Identity),
          QueryAddress: net.IP(msg.QueryAddress).String(),
          ResponseTime: responseTime,
          ResponseStatus: responseStatus,
          QuestionName: question.Name,
          QuestionType: dns.Type(question.Qtype).String(),
          Counter: 1,
        }

        log.Debug.Printf("%s", r)
        d.WriteResponseChannel <- r
        d.ResponseCounter++
        log.Debug.Printf("Dnstap write response: %s", r)
      }
    default:
      continue
    }

  }
}

func (d *Dnstap) Stats() (uint, uint) {
  queryCounter := d.QueryCounter
  responseCounter := d.ResponseCounter
  d.QueryCounter = 0
  d.ResponseCounter = 0
  return queryCounter, responseCounter
}
