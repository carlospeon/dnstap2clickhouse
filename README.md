dnstap2clickhouse - read dnstap messages and write them to clickhouse.

Install
=======

* Check [Releases](../../releases) for rpm packages.
* Or install from *copr*
```
dnf copr enable carlospeon/dnstap2clickhouse
```

Build
=====

```
go build -o dnstap2clickhouse src/main.go
```

Documentation
=============

Man page at [doc/dnstap2clickhouse.md](../../blob/main/doc/dnstap2clickhouse.md).

QuickStart for *bind* at [doc/quickstart.md](../../blob/main/doc/quickstart.md).
