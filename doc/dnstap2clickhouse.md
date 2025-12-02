% dnstap2clickhouse(7) | dnstap2clickhouse

NAME
====

dnstap2clickhouse - read dnstap messages and write them to clickhouse.

SYNOPSIS
========

`dnstap2clickhouse [-config config-file] [-loglevel trace|debug|info|warn|error]`

DESCRIPTION
===========

dnstap2clickhouse reads *dnstap*  *"QueryMessages"* from *unixsocket* and write them to *clickhouse* in batchs.
Some capabilities are:

* Aggregate duplicated messages writting an extra column *ColumnCounter* with the number of occurences. 
* Aggregate messages grouping by *QueryAddress*
* Aggregate messages grouping by *QuestionName* and *QuestionType*

OPTIONS
=======

`-config CONFIG_FILE`

    Configuration file in *toml* format. Defaults to */etc/dnstap2clickhouse.conf*

`-loglevel trace|debug|info|warn|error`

    Defaults to *info*

FILES
=====

* /etc/dnstap2clickhouse.conf. Defaults to:

```
# LogLevel. Log levels are trace, debug, info, warn, error
LogLevel = "info"

[Aggregator]
# WriteInterval. Interval between writes to clickhouse
WriteInterval = "20s"

# Aggregate. Enable deduplication of messages and aggregation functions if true.
# When false write one row for every message.
Aggregate = true

# WriteUngrouped. When Aggregation is true write a row for deduplicated messages.
WriteUngrouped = true

# GroupbyQueryAddress. When Aggregation is true write a row for the aggregation of 
# queries grouped by QueryAddress
GroupbyQueryAddress = true

# GroupbyQuestion. When Aggregation is true write a row for the aggregation of 
# queries grouped by QuestionName
GroupbyQuestion = true

[ClickHouse]
# Connection options
Host = "localhost"
Port = 9000
Username = "default"
Password = ""
Database = "default"

# QueryTable. Table to insert client queries.
# Set to empty string ("") to avoid writting client queries.
QueryTable = "clientQuery"
# ResponseTable. Table to insert client responses.
# Set to empty string ("") to avoid writting client responses.
ResponseTable = "clientResponse"
# Column names
QueryTimeColumn = "queryTime"
ResponseTimeColumn = "responseTime"
ResponseStatusColumn = "responseStatus"
IdentityColumn = "identity"
QueryAddressColumn = "queryAddress"
QuestionNameColumn = "questionName"
QuestionTypeColumn = "questionType"
# CounterColumn. Only if Aggregate is true
CounterColumn = "counter"

[Dnstap]
# UnixSocket. Where to read dnstap messages from.
# Bind example "/run/named/dnstap.sock"
UnixSocket = "dnstap.sock"
ReadTimeout = "5s"
# Readers. Number of goroutines reading UnixSocket
Readers = 1
```

AUTHOR
=====
Carlos Peón (carlospeon@gmail.com)
