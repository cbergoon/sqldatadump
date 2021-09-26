<h1 align="center">sqldatadump</h1>
<p align="center">
<a href="https://godoc.org/github.com/cbergoon/sqldatadump"><img src="https://img.shields.io/badge/godoc-reference-brightgreen.svg" alt="Docs"></a>
<a href="#"><img src="https://img.shields.io/badge/version-0.1.0-brightgreen.svg" alt="Version"></a>
</p>

`sqldatadump` generates insert statements with data for tables in a provided SQL Server database. 

#### Documentation 

See the docs [here](https://godoc.org/github.com/cbergoon/sqldatadump).

#### Install
```
go install github.com/cbergoon/sqldatadump
```

#### Example Usage

```
USAGE: Usage: sqldatadump [--directory] [--schema=<schema>] [--batchesPerFile=<batches>] [--rowsPerBatch=<rows>] <username>:<password>@<address>:<port>/<database>
```

To generate insert statements in a directory called `table-data` for tables in the `dbo` schema of the `AdventureWorks` database with 10 batches per file and 1000 records per batch: 

```
$ sqldatadump --directory=./table-data --schema=dbo --rowsPerBatch=1000 --batchesPerFile=10 --ignoreTables=CarrierRef,MasterLocation '<username>:<password>@127.0.0.1:1433/AdventureWorks'
```

To use windows authentication:

```
$ sqldatadump --directory=./table-data --schema=dbo --rowsPerBatch=1000 --batchesPerFile=10 --ignoreTables=CarrierRef,MasterLocation '<domain>\<username>:<password>@127.0.0.1:1433/AdventureWorks'
```
#### License
This project is licensed under the MIT License.







