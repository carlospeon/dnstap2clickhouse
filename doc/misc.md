
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

Client resposes table example:
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

