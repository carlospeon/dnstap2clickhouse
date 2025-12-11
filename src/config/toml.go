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

package config

import (
  "errors"
  "reflect"
  "time"
  
  "github.com/BurntSushi/toml"

  "dnstap2clickhouse/src/aggregator"
  "dnstap2clickhouse/src/clickhouse"
  "dnstap2clickhouse/src/dnstap"
  "dnstap2clickhouse/src/log"
)

type Config struct {
  LogLevel string
  Aggregator aggregator.Config
  Dnstap dnstap.Config
  ClickHouse clickhouse.Config
}

// Default values
var Defaults = Config {
  LogLevel: "info",
  Aggregator: aggregator.Config{
    WriteInterval: 20 * time.Second,
    Aggregate: true,
    WriteUngrouped: true,
    GroupbyQueryAddress: true,
    GroupbyQuestion: true,
  },
  ClickHouse: clickhouse.Config{
    Hosts: "localhost:9000",
    Secure: false,
    InsecureSkipVerify: false,
    Username: "default",
    Password: "",
    Database: "default",
    QueryTable: "clientQuery",
    ResponseTable: "clientResponse",
    QueryTimeColumn: "queryTime",
    ResponseTimeColumn: "responseTime",
    ResponseStatusColumn: "responseStatus",
    IdentityColumn: "identity",
    QueryAddressColumn: "queryAddress",
    QuestionNameColumn: "questionName",
    QuestionTypeColumn: "questionType",
    CounterColumn: "counter",
  },
  Dnstap: dnstap.Config {
    UnixSocket: "/run/named/dnstap.sock",
    ReadTimeout: 5 * time.Second,
    Readers: 1,
    ClientQueries: true,
    NonOkClientResponses: true,
  },
}

func printField(logger *log.Logger, prefix string, f any) {
  switch f.(type) {
  case bool:
    logger.Printf("%s: %t", prefix, f)
  case int, int32, int64:
    logger.Printf("%s: %d", prefix, f)
  case float32, float64:
    logger.Printf("%s: %f", prefix, f)
  default:
    logger.Printf("%s: %s", prefix, f)
  }
}

func printStructFields(logger *log.Logger, s any, prefix string) {
  rv := reflect.ValueOf(s)
  rt := reflect.TypeOf(s)

  if rv.Kind() == reflect.Ptr {
    rv = rv.Elem()
    rt = rt.Elem()
  }

  if rv.Kind() != reflect.Struct {
    log.Debug.Printf("Not a struct: %s", rv.Kind())
    return
  }

  for i := 0; i < rv.NumField(); i++ {
    fieldValue := rv.Field(i)
    fieldType := rt.Field(i)

    log.Trace.Printf("Print Kind: %s", fieldValue.Kind())
    if fieldValue.Kind() == reflect.Struct {
      log.Debug.Printf("Print Config struct: %s", fieldType.Name)
      printStructFields(logger, fieldValue.Interface(), prefix + " " + fieldType.Name)
    } else {
      if fieldType.Name != "Password" {
        printField(logger, prefix + " " + fieldType.Name, fieldValue.Interface())
      }
    }

  }
}

func patchStructFields(s any, m map[string]any) (error) {
  rv := reflect.ValueOf(s)
  rt := reflect.TypeOf(s)

  if rv.Kind() == reflect.Ptr {
    rv = rv.Elem()
    rt = rt.Elem()
  }

  if rv.Kind() != reflect.Struct {
    return errors.New("Patch Struckt not a struct")
  }

  for i := 0; i < rv.NumField(); i++ {
    fieldValue := rv.Field(i)
    fieldType := rt.Field(i)

    value, ok := m[fieldType.Name]
    if !ok {
      continue
    }

    log.Trace.Printf("Patch Kind: %s", fieldValue.Kind())
    if fieldValue.Kind() == reflect.Struct {
      log.Debug.Printf("Patch Config struct: %s", fieldType.Name)
      patchStructFields(fieldValue.Interface(), value.(map[string]any))
    } else {
      log.Debug.Printf("Patch Config %s: %s", fieldType.Name, value)
      val := reflect.ValueOf(value)
      if !val.Type().AssignableTo(fieldType.Type) {
        return errors.New("Provided value type not assignable to obj field type")
      }
      fieldValue.Set(val)
    }
  }
  return nil
}

func Load(args map[string]any, filePath string) (Config, error) {
  config := Defaults
  tDec, err := toml.DecodeFile(filePath, &config)
  if err != nil {
    log.Error.Printf("Error loading config file %s", filePath)
    return config, err
  }
  log.Trace.Printf("toml decode: %s", tDec)
  printStructFields(log.Debug, &config, "File Config") 

  err = patchStructFields(&config, args)
  if err != nil {
    return config, err
  }
  log.Trace.Printf("toml config loaded: %s", config)

  printStructFields(log.Info, &config, "Config") 

  if !config.Aggregator.Aggregate {
    config.ClickHouse.CounterColumn = ""
  }
  if !config.Dnstap.ClientQueries {
    config.ClickHouse.QueryTable = ""
  }
  if !config.Dnstap.NonOkClientResponses {
    config.ClickHouse.ResponseTable = ""
  }

  return config, nil

}
