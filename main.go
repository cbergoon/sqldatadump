package main

import (
	"database/sql"
	"flag"
	"fmt"
	"log"
	"math"
	"os"
	"path/filepath"
	"strings"
	"time"

	_ "github.com/denisenkom/go-mssqldb"
	mssql "github.com/denisenkom/go-mssqldb"
	"github.com/jmoiron/sqlx"
)

type config struct {
	Directory      string `json:"directory"`
	RowsPerBatch   int    `json:"rowsPerBatch"`
	BatchesPerFile int    `json:"batchesPerFile"`
	IgnoreTables   string `json:"ignoreTables"`
	DbAddress      string `json:"dbAddress"`
	DbPort         string `json:"dbPort"`
	DbUsername     string `json:"dbUsername"`
	DbPassword     string `json:"dbPassword"`
	DbDatabase     string `json:"dbDatabase"`
	DbSchema       string `json:"dbSchema"`
}

type RawTableData struct {
	TableCatalog          string `db:"TABLE_CATALOG"`
	TableSchema           string `db:"TABLE_SCHEMA"`
	TableName             string `db:"TABLE_NAME"`
	ColumnName            string `db:"COLUMN_NAME"`
	ColumnOrdinalPosition int    `db:"ORDINAL_POSITION"`
	ColumnIsNullable      string `db:"IS_NULLABLE"`
	ColumnDatatype        string `db:"DATA_TYPE"`
}

type Column struct {
	Name            string
	OrdinalPosition int
	IsNullable      string
	Datatype        string
}

type ExportTableMetadata struct {
	TableCatalog    string
	TableSchema     string
	TableName       string
	Columns         []*Column
	ColumnMap       map[string]*Column
	SelectStatement string
}

func generateCommentBlock(address, database, schema string) string {
	block := `/* 
	Data Dump Created by sqldatadump

	Data Exported from %s/%s/%s
*/

`
	return fmt.Sprintf(block, address, database, schema)
}

type ObjectDefinition struct {
	Definition []string
}

func (od *ObjectDefinition) String() string {
	return strings.Join(od.Definition, "")
}

func isValidConnectionString(connectionDetails string) bool {
	if strings.Count(connectionDetails, ":") < 2 {
		return false
	}
	if strings.Count(connectionDetails, "@") < 1 {
		return false
	}
	if strings.Count(connectionDetails, "/") < 1 {
		return false
	}
	return true
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: sqldatadump [--directory] [--schema=<schema>] [--batchesPerFile=<batches>] [--rowsPerBatch=<rows>] <username>:<password>@<address>:<port>/<database>\n")
	}

	cfg := &config{}

	flag.StringVar(&cfg.Directory, "directory", "", "root directory to export to")
	flag.StringVar(&cfg.DbSchema, "schema", "", "schema to export")
	flag.IntVar(&cfg.BatchesPerFile, "batchesPerFile", 10, "number insert batches per file")
	flag.IntVar(&cfg.RowsPerBatch, "rowsPerBatch", 1000, "number rows to insert per batch")
	flag.StringVar(&cfg.IgnoreTables, "ignoreTables", "", "comma separated list of tables that should be ignored")

	flag.Parse()

	// expecting string of form <username>:<password>@<address>:<port>/<database>
	args := flag.Args()
	if len(args) != 1 {
		flag.Usage()
		os.Exit(-1)
	}

	if cfg.Directory == "" {
		flag.Usage()
		os.Exit(-1)
	}

	if cfg.BatchesPerFile <= 0 || cfg.RowsPerBatch <= 0 {
		flag.Usage()
		os.Exit(-1)
	}

	newpath := filepath.Join(cfg.Directory)
	err := os.MkdirAll(newpath, os.ModePerm)
	if err != nil {
		log.Fatal(err)
	}

	connectionDetails := args[0]
	if !isValidConnectionString(connectionDetails) {
		flag.Usage()
		os.Exit(-1)
	}

	cfg.DbUsername = connectionDetails[:strings.Index(connectionDetails, ":")]
	cfg.DbPassword = connectionDetails[strings.Index(connectionDetails, ":")+1 : strings.Index(connectionDetails, "@")]
	cfg.DbAddress = connectionDetails[strings.Index(connectionDetails, "@")+1 : strings.LastIndex(connectionDetails, ":")]
	cfg.DbPort = connectionDetails[strings.LastIndex(connectionDetails, ":")+1 : strings.LastIndex(connectionDetails, "/")]
	cfg.DbDatabase = connectionDetails[strings.Index(connectionDetails, "/")+1:]

	mssqldb, err := sqlx.Connect("sqlserver", fmt.Sprintf("server=%s;user id=%s;password=%s;database=%s;port=%s", cfg.DbAddress, cfg.DbUsername, cfg.DbPassword, cfg.DbDatabase, cfg.DbPort))
	if err != nil {
		log.Fatal(err)
	}

	var objects []*RawTableData

	ignoreTables := strings.Split(cfg.IgnoreTables, ",")
	for i := 0; i < len(ignoreTables); i++ {
		ignoreTables[i] = "'" + strings.TrimSpace(ignoreTables[i]) + "'"
	}
	ignoreTablesInItems := strings.Join(ignoreTables, ", ")
	fmt.Println(ignoreTablesInItems)

	rows, err := mssqldb.Query(`SELECT c.[TABLE_CATALOG], c.[TABLE_SCHEMA], c.[TABLE_NAME], c.[COLUMN_NAME], c.[ORDINAL_POSITION], c.[IS_NULLABLE], c.[DATA_TYPE]
	FROM INFORMATION_SCHEMA.COLUMNS c 
	JOIN INFORMATION_SCHEMA.TABLES t ON c.[TABLE_NAME] = t.[TABLE_NAME]
	WHERE t.[TABLE_TYPE] = 'BASE TABLE' 
		AND t.[TABLE_NAME] NOT IN (`+ignoreTablesInItems+`)
		AND c.[TABLE_SCHEMA] = @p1
	ORDER BY c.[TABLE_SCHEMA], c.[TABLE_NAME], c.[ORDINAL_POSITION] `, cfg.DbSchema)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()
	for rows.Next() {
		obj := &RawTableData{}
		err := rows.Scan(&obj.TableCatalog, &obj.TableSchema, &obj.TableName, &obj.ColumnName, &obj.ColumnOrdinalPosition, &obj.ColumnIsNullable, &obj.ColumnDatatype)
		if err != nil {
			log.Fatal(err)
		}
		objects = append(objects, obj)
	}
	err = rows.Err()
	if err != nil {
		log.Fatal(err)
	}

	tables := []*ExportTableMetadata{}
	prevIdent := ""
	for _, o := range objects {
		if fmt.Sprintf("%s.%s.%s", o.TableCatalog, o.TableSchema, o.TableName) != prevIdent {
			prevIdent = fmt.Sprintf("%s.%s.%s", o.TableCatalog, o.TableSchema, o.TableName)
			tables = append(tables, &ExportTableMetadata{
				TableCatalog: o.TableCatalog,
				TableSchema:  o.TableSchema,
				TableName:    o.TableName,
				Columns:      []*Column{},
				ColumnMap:    map[string]*Column{},
			})
		}
		tables[len(tables)-1].Columns = append(tables[len(tables)-1].Columns, &Column{
			Name:            o.ColumnName,
			OrdinalPosition: o.ColumnOrdinalPosition,
			IsNullable:      o.ColumnIsNullable,
			Datatype:        o.ColumnDatatype,
		})

		tables[len(tables)-1].ColumnMap[o.ColumnName] = &Column{
			Name:            o.ColumnName,
			OrdinalPosition: o.ColumnOrdinalPosition,
			IsNullable:      o.ColumnIsNullable,
			Datatype:        o.ColumnDatatype,
		}
	}

	for _, table := range tables {
		s := time.Now()
		columns := ""
		for i, col := range table.Columns {
			if i == 0 {
				columns += "[" + col.Name + "]"
			} else {
				columns += ", [" + col.Name + "]"
			}
		}
		table.SelectStatement = fmt.Sprintf("SELECT %s FROM [%s].[%s].[%s]", columns, table.TableCatalog, table.TableSchema, table.TableName)

		rows, err := mssqldb.Queryx(table.SelectStatement)
		if err != nil {
			log.Fatal(err)
		}

		log.Default().Printf("exported table data for [%s].[%s].[%s] in %s", table.TableCatalog, table.TableSchema, table.TableName, time.Since(s))
		s1 := time.Now()
		resarr := []map[string]interface{}{}

		cs, _ := rows.Columns()
		csl := len(cs)
		for rows.Next() {
			in := make(map[string]interface{}, csl)
			err = rows.MapScan(in)
			if err != nil {
				log.Fatal(err)
			}
			resarr = append(resarr, in)
		}

		if rows.Err() != nil {
			switch rows.Err() {
			case sql.ErrNoRows:
				break
			default:
				log.Fatal(err)
			}
		}

		log.Default().Printf("retrieve table data for [%s].[%s].[%s] in %s", table.TableCatalog, table.TableSchema, table.TableName, time.Since(s1))
		s2 := time.Now()

		insertStatements := []string{}
		insert := fmt.Sprintf("INSERT INTO [%s].[%s].[%s] (%s) VALUES ", table.TableCatalog, table.TableSchema, table.TableName, columns)

		datachunks := chunkMapInterfaceSlice(resarr, cfg.RowsPerBatch)
		for _, dc := range datachunks {
			ni := insert
			for i, r := range dc {
				niv := ""
				for k, c := range table.Columns {
					if k != 0 {
						niv += ", "
					}
					niv += insertStringValue(r[c.Name])
				}
				if i == 0 {
					ni += fmt.Sprintf("\n  (%s) \n", niv)
				} else {
					ni += fmt.Sprintf(", (%s) \n", niv)
				}
			}
			insertStatements = append(insertStatements, ni+"\n")
		}

		log.Default().Printf("created insert statements for [%s].[%s].[%s] in %s", table.TableCatalog, table.TableSchema, table.TableName, time.Since(s2))
		s3 := time.Now()

		filechunks := chunkStringSlice(insertStatements, cfg.BatchesPerFile)
		for i, chunk := range filechunks {
			idionoff := fmt.Sprintf("SET IDENTITY_INSERT [%s].[%s] ON\n\n %s \n\nSET IDENTITY_INSERT [%s].[%s] OFF", table.TableSchema, table.TableName, strings.Join(chunk, " \n\n "), table.TableSchema, table.TableName)

			err = os.WriteFile(filepath.Join(newpath, fmt.Sprintf("%s_%s_%s_%d.sql", table.TableCatalog, table.TableSchema, table.TableName, i+1)), []byte(idionoff), 0644)
			if err != nil {
				log.Fatal(err)
			}
		}

		log.Default().Printf("wrote insert statements for [%s].[%s].[%s] in %s", table.TableCatalog, table.TableSchema, table.TableName, time.Since(s3))

		log.Default().Printf("completed data dump for [%s].[%s].[%s] in %s", table.TableCatalog, table.TableSchema, table.TableName, time.Since(s))
	}
}

func chunkMapInterfaceSlice(s []map[string]interface{}, size int) [][]map[string]interface{} {
	res := [][]map[string]interface{}{}

	fullchunks := math.Floor(float64(len(s)) / float64(size))
	partialchunk := len(s) % size

	for i := 0; i < int(fullchunks); i++ {
		res = append(res, s[i*size:(i*size)+size])
	}

	if partialchunk > 0 {
		res = append(res, s[int(fullchunks)*size:])
	}

	return res
}

func chunkStringSlice(s []string, size int) [][]string {
	res := [][]string{}

	fullchunks := math.Floor(float64(len(s)) / float64(size))
	partialchunk := len(s) % size

	for i := 0; i < int(fullchunks); i++ {
		res = append(res, s[i*size:(i*size)+size])
	}

	if partialchunk > 0 {
		res = append(res, s[int(fullchunks)*size:])
	}

	return res
}

func insertStringValue(val interface{}) string {
	i := ""
	if val == nil {
		return "NULL"
	}
	switch t := val.(type) {
	case int:
		i = fmt.Sprintf("%d", t)
	case int8:
		i = fmt.Sprintf("%d", t)
	case int16:
		i = fmt.Sprintf("%d", t)
	case int32:
		i = fmt.Sprintf("%d", t)
	case int64:
		i = fmt.Sprintf("%d", t)
	case float32:
		i = fmt.Sprintf("%f", t)
	case float64:
		i = fmt.Sprintf("%f", t)
	case uint8:
		i = fmt.Sprintf("%d", t)
	case uint16:
		i = fmt.Sprintf("%d", t)
	case uint32:
		i = fmt.Sprintf("%d", t)
	case uint64:
		i = fmt.Sprintf("%d", t)
	case string:
		i = fmt.Sprintf("'%s'", strings.ReplaceAll(t, "'", "''"))
	case bool:
		if t {
			i = "1"
		} else {
			i = "0"
		}
	case time.Time:
		i = fmt.Sprintf("'%s'", t.Format(time.RFC3339))
	case mssql.DateTime1:
		i = fmt.Sprintf("'%s'", t)
	case mssql.DateTimeOffset:
		i = fmt.Sprintf("'%s'", t)
	case []uint8:
		i = fmt.Sprintf("%s", t)
	default:
		i = fmt.Sprintf("'%s'", t)
	}
	return i
}
