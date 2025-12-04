Install
=======
Install rpm from GH releases or from copr:
```
dnf copr enable carlospeon/dnstap2clickhouse
dnf -y install dnstap2clickhouse

```
Configuration
=============

Set DNS server user as runtime for *dnstap2clickhouse.service*
```
### Editing /etc/systemd/system/dnstap2clickhouse.service.d/override.conf
### Anything between here and the comment below will become the new contents of the file
[Service]
User=_DNS_USER_
Group=_DNS_GROUP_

### Lines below this comment will be discarded
```

Set UnixSocket location
```
# /etc/dnstap2clickhouse.conf
UnixSocket = "_DNS_UNIXSOCKET_"
```

Allow DNS server user to read config
```
chgrp _DNS_USER_ /etc/dnstap2clickhouse.conf
```

Run dnstap2clickhouse confined within DNS context
```
semanage fcontext -d -t named_exec_t '/usr/bin/dnstap2clickhouse'
semanage fcontext -a -t _DNS_CONTEXT_ '/usr/bin/dnstap2clickhouse'
restorecon -F /usr/bin/dnstap2clickhouse
```

Bind
====

Config options to send client queries to dnstap unixsocket:
```
options {
  // ...
  dnstap { client query; client response; };
  dnstap-output unix "/run/named/dnstap.sock";
  //  ...  
}
```

ClickHouse
==========

Client queries table example:
```
CREATE TABLE clientQuery (
  queryTime DateTime64(3),
  identity String,
  queryAddress String,
  questionName String,
  questionType String,
  counter UInt64)
ENGINE = Memory
SETTINGS min_rows_to_keep = 100, max_rows_to_keep = 1000;
```

Client responses table example:
```
CREATE TABLE clientResponse (
  responseTime DateTime64(3),
  identity String,
  responseStatus String,
  queryAddress String,
  questionName String,
  questionType String,
  counter UInt64)
ENGINE = Memory
SETTINGS min_rows_to_keep = 100, max_rows_to_keep = 1000;
```

Grafana
=======

Top queryAddress bar chart:
```
SELECT
    queryAddress,
    sum(counter) as values
FROM dnstap.clientQuery
WHERE
    questionName = '__ANY__' and
    questionType = '__ANY__' and
    $__timeFilter(queryTime)
GROUP BY queryAddress
ORDER BY values DESC
LIMIT 10
```

Top *NXDOMAIN* bar chart:
```
SELECT
    questionName || ' ' || questionType,
    sum(counter) as values
FROM dnstap.clientResponse
WHERE
    responseStatus == 'NXDOMAIN' and
    queryAddress = '__ANY__' and
    $__timeFilter(responseTime)
GROUP BY responseStatus, questionName, questionType
ORDER BY values DESC
LIMIT 10
```

Non OK Responses time series:
```
SELECT
    $__timeInterval(responseTime) AS responseTimeInterval,
    responseStatus || ' ' || questionName || ' ' questionType,
    sum(counter) as counter
FROM dnstap.clientResponse
WHERE
    queryAddress = '__ANY__' and
    $__timeFilter(responseTimeInterval)
GROUP BY responseTimeInterval, responseStatus, questionName, questionType
ORDER BY responseTimeInterval ASC
```

