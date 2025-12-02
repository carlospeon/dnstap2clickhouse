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

package log

import (
  "errors"
  "io"
  "log"
)


type Logger = log.Logger
type LogLevel int

const (
  TraceLevel    LogLevel = iota + 1 // EnumIndex = 1
  DebugLevel                        // EnumIndex = 2
  InfoLevel                         // EnumIndex = 3
  WarnLevel                         // EnumIndex = 4
  ErrorLevel                        // EnumIndex = 5
)


var (
  Trace *log.Logger
  Debug *log.Logger
  Info  *log.Logger
  Warn  *log.Logger
  Error *log.Logger

  Level LogLevel
  LevelString string
)


func IsTrace() bool {
  return TraceLevel >= Level
}
func IsDebug() bool {
  return DebugLevel >= Level
}

func Init(handler io.Writer, levelString string) error {

  switch levelString {
  case "trace":
    Level = TraceLevel
  case "debug":
    Level = DebugLevel
  case "info":
    Level = InfoLevel
  case "warn":
    Level = WarnLevel
  case "error":
    Level = ErrorLevel
  default:
    return errors.New("Unknown LogLevel")
  }

  LevelString = levelString

  traceHandler := io.Discard
  debugHandler := io.Discard
  infoHandler := io.Discard
  warnHandler := io.Discard
  errorHandler := io.Discard

  flags := 0

  if IsTrace() {
    traceHandler = handler
    flags = flags | log.Lshortfile
  }
  if IsDebug() {
    debugHandler = handler
    flags = flags | log.Lshortfile
  }
  if InfoLevel >= Level {
    infoHandler = handler
  }
  if WarnLevel >= Level {
    warnHandler = handler
  }
  if ErrorLevel >= Level {
    errorHandler = handler
  }

  Trace = log.New(traceHandler, "TRACE ", flags)
  Debug = log.New(debugHandler, "DEBUG ", flags)
  Info = log.New(infoHandler, "INFO ", flags)
  Warn = log.New(warnHandler, "WARN ", flags)
  Error = log.New(errorHandler, "ERROR ", flags)

  return nil
}

func ReInit(handler io.Writer, levelString string) error {
  if levelString != LevelString {
    return Init(handler, levelString)
  }
  return nil
}

