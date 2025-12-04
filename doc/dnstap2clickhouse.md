% dnstap2clickhouse(7) | dnstap2clickhouse

NAME
====

dnstap2clickhouse - read dnstap messages and write them to clickhouse.

SYNOPSIS
========

`dnstap2clickhouse [-config CONFIG_FILE] [-loglevel trace|debug|info|warn|error]`

DESCRIPTION
===========

dnstap2clickhouse reads *dnstap* client *"QueryMessages"* and non OK (ie. *"NXDOMAIN"*,
*"SERVFAIL"*, etc.) client *"ResponseMessages"* from *unixsocket* and write them to
*clickhouse* in batchs. Some capabilities are:

* Aggregate duplicated messages writing an extra column *CounterColumn* with the number
of occurences.
* Aggregate messages grouping by *QueryAddress*
* Aggregate messages grouping by *QuestionName* and *QuestionType*

OPTIONS
=======

**-config** _CONFIG_FILE_

    Configuration file in *toml* format. Defaults to */etc/dnstap2clickhouse.conf*

**-loglevel trace|debug|info|warn|error**

    Defaults to *info*

FILES
=====

**/etc/dnstap2clickhouse.conf**. Default configuration:

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
QueryTable = "clientQuery"
# ResponseTable. Table to insert client responses.
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
UnixSocket = "/run/named/dnstap.sock"
ReadTimeout = "5s"
# Readers. Number of goroutines reading UnixSocket
Readers = 1
# ClientQueries. Process client queries
ClientQueries = true
# NonOkClientResponses. Process non OK client responses
NonOkClientResponses = true
```

CAVEATS
=======

Out of the box dnstap2clickhouse is installed to run with *bind*, that is,
it will run with *named* user and within the same SELinux context.

If you plan to use with other DNS servers take following into consideration:

**UnixSocket location, owner and permissions**.
DNS server process must be able to write to it, so:
* Choose an appropriate directory, specially if DNS server runs confined
by *SELinux*.
* Set an approriate *systemd* user for dnstap2clickhouse. Tipically same
user as the DNS server is recommended.

**Configuration permissions**
Configuration store ClickHouse password so is not world accesible by default.
Set a proper owner/group to it.

**Startup order/dependecies**.
Tipically dnstap2clickhouse should start before the DNS server in order to
create the *UnixSocket* prior to DNS start up, but convinient directories
to create it maybe are not available before DNS start up.

Some DNS servers are able to reconnect later to the socket and
dnstap2clickhouse will retry to create de *UnixSocket* if the directory
is not avaiable during startup but depending on the DNS server the apropiate
startup order may differ.

AUTHOR
======
Carlos Peón (carlospeon@gmail.com)

