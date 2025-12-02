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

package main

import (
  "context"
  "flag"
  "os"
  "os/signal"
  "sync"
  "syscall"
  "time"

  "dnstap2clickhouse/src/log"
  "dnstap2clickhouse/src/config"
  "dnstap2clickhouse/src/aggregator"
  "dnstap2clickhouse/src/clickhouse"
  "dnstap2clickhouse/src/dnstap"
)

const MAX_STATS_INTERVAL = 4 * time.Hour
const UNDEF_STRING = "UNDEF"

var cfg config.Config = config.Defaults

func main() {
  var (
    configFile string
    argsCfg config.Config
  )

  flag.StringVar(&argsCfg.LogLevel, "loglevel", UNDEF_STRING, "log level: trace|debug|info|warn|error")
  flag.StringVar(&configFile, "config", "/etc/dnstap2clickhouse.conf", "path to file in toml format")
  flag.Parse()

  args := make(map[string]any)
  if argsCfg.LogLevel != UNDEF_STRING {
    cfg.LogLevel = argsCfg.LogLevel
    args["LogLevel"] = argsCfg.LogLevel
  }

  err := log.Init(os.Stdout, cfg.LogLevel)
  if err != nil {
    panic(err)
  }

  log.Info.Printf("Loading config: %s", configFile)
  cfg, err = config.Load(args, configFile)
  if err != nil {
    log.Error.Printf("%s\n", err)
    os.Exit(1)
  }

  err = log.ReInit(os.Stdout, cfg.LogLevel)
  if err != nil {
    panic(err)
  }

  ctx, cancel := context.WithCancel(context.Background())
  defer cancel()
  go initSignals(cancel)

  err = run(ctx)
  if err != nil {
    log.Debug.Printf("%s\n", err)
    cancel()
    os.Exit(1)
  }
  log.Info.Printf("Exit.\n")
}

func print(e error) {
  if e != nil {
    log.Error.Printf("%s\n.", e)
  }
}

func initSignals(cancel context.CancelFunc) {
  var captureSignal = make(chan os.Signal, 1)
  signal.Notify(captureSignal, syscall.SIGTERM,
                               syscall.SIGQUIT,
                               syscall.SIGABRT,
                               syscall.SIGINT)
  signalHandler(<-captureSignal, cancel)
}

func signalHandler(signal os.Signal, cancel context.CancelFunc) {
  log.Warn.Printf("Caught signal: %+v.\n", signal)

  switch signal {
  case syscall.SIGTERM,
       syscall.SIGQUIT,
       syscall.SIGABRT,
       syscall.SIGINT:
    log.Warn.Printf("Canceling.\n")
    cancel()
  }
}

func run(ctx context.Context) error {
  var err error = nil
  var wg sync.WaitGroup

  ch := clickhouse.Init(
    &clickhouse.ClickHouse{ Config: cfg.ClickHouse },
  )
  wg.Add(1)
  go ch.Run(ctx, &wg)

  aggr := aggregator.Init(
    &aggregator.Aggregator{
      Config: cfg.Aggregator,
      WriteQueryChannel: ch.QueryChannel,
      WriteResponseChannel: ch.ResponseChannel,
    },
  )
  wg.Add(1)
  go aggr.Run(ctx, &wg)

  dt, err := dnstap.Init(
    &dnstap.Dnstap{ 
      Config: cfg.Dnstap,
      WriteQueryChannel: aggr.ReadQueryChannel,
      WriteResponseChannel: aggr.ReadResponseChannel,
    },
  )
  if err != nil {
    log.Error.Printf("%s", err)
    return err
  }
  wg.Add(1)
  go dt.Listen(ctx, &wg)
  for i := 0; i < cfg.Dnstap.Readers; i++ {
    wg.Add(1)
    go dt.Read(ctx, &wg)
  }

  statsInterval := 10 * time.Second
  timer := time.NewTimer(statsInterval)

  for {
    select {
    case <-timer.C:
      if statsInterval < MAX_STATS_INTERVAL {
        statsInterval += statsInterval / 2
      }
      timer.Reset(statsInterval)
      dtQueryCounter, dtResponseCounter := dt.Stats()
      aggrQueryCounter, aggrResponseCounter := aggr.Stats()
      chQueryCounter, chResponseCounter := ch.Stats()
      log.Info.Printf("Dnstap query/response: %d/%d, " +
                      "Aggregator query/response: %d/%d, " +
                      "ClickHouse query/response: %d/%d",
                      dtQueryCounter, dtResponseCounter,
                      aggrQueryCounter, aggrResponseCounter,
                      chQueryCounter, chResponseCounter)

    case <-ctx.Done():
      dt.Close()
      wg.Wait()
      return err
    }
  }
}

