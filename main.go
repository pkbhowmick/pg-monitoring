package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pkbhowmick/pg-monitoring/model"
	"github.com/pkbhowmick/pg-monitoring/pkg/database"
	"github.com/tidwall/pretty"
)

var (
	DBConnectionConfig = os.Getenv("DB_URL")
)

func GetStatements(db *sql.DB) ([]model.Statement, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	q := `SELECT userid, dbid, queryid, calls, query, total_exec_time, min_exec_time, max_exec_time
          FROM pg_stat_statements
          ORDER BY total_exec_time DESC
          LIMIT 10`
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var statements []model.Statement

	for rows.Next() {
		var s model.Statement
		err := rows.Scan(&s.UserOID, &s.DBOID, &s.QueryID, &s.Calls, &s.Query, &s.TotalTime, &s.MinTime, &s.MaxTime)
		if err != nil {
			return nil, err
		}
		statements = append(statements, s)
	}
	return statements, nil

}

func GetDatabases(db *sql.DB) ([]model.Database, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	q := `SELECT D.oid, D.datname, D.datdba, D.dattablespace, s.numbackends
			FROM pg_database AS D JOIN pg_stat_database AS S ON D.oid = S.datid
			WHERE (NOT D.datistemplate)
			ORDER BY D.oid ASC`
	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}
	var databases []model.Database
	for rows.Next() {
		var d model.Database
		err := rows.Scan(&d.OID, &d.Name, &d.DatDBA, &d.DatTableSpace, &d.NumBackends)
		if err != nil {
			return nil, err
		}
		databases = append(databases, d)
	}
	return databases, nil
}

func GetTablesInfo(db *sql.DB) ([]model.Table, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	q := `SELECT relid, schemaname, relname, current_database(), n_tup_ins, n_live_tup
			FROM pg_stat_user_tables
			ORDER BY relid ASC`

	rows, err := db.QueryContext(ctx, q)
	if err != nil {
		return nil, err
	}

	var tables []model.Table

	for rows.Next() {
		var t model.Table

		err := rows.Scan(&t.OID, &t.SchemaName, &t.Name, &t.DBName, &t.RowsInserted, &t.RowsLive)
		if err != nil {
			return nil, err
		}

		tables = append(tables, t)
	}
	return tables, nil
}

func GetJsonMetrics() ([]byte, error) {
	connConfig := database.GetDefaultCollectConfig()
	db, err := database.GetDBConnection(DBConnectionConfig, connConfig)
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	log.Println("Successfully connected to databases")

	var model model.Model

	model.Statements, err = GetStatements(db)
	if err != nil {
		return nil, err
	}

	model.Databases, err = GetDatabases(db)
	if err != nil {
		return nil, err
	}

	model.Tables, err = GetTablesInfo(db)
	if err != nil {
		return nil, err
	}

	data, err := json.Marshal(model)
	if err != nil {
		return nil, err
	}
	jsonStr := pretty.Pretty(data)
	return jsonStr, nil
}

func GetPromMetrics(res http.ResponseWriter, req *http.Request) {
	res.Write([]byte(`Prometheus metrics will come here`))
}

func NewConnection() (nc *nats.Conn, err error) {
	servers := os.Getenv("NATS_URL")

	if servers == "" {
		return nil, fmt.Errorf("no server is specified. Specify a server to connect to using NATS_URL")
	}

	for {
		nc, err := nats.Connect(servers)
		if err == nil {
			return nc, nil
		}

		log.Printf("could not connect to NATS: %s\n", err)
		time.Sleep(500 * time.Millisecond)
	}
}

func main() {
	nc, err := NewConnection()
	if err != nil {
		log.Fatalln(err)
	}
	for {
		jsonObj, err := GetJsonMetrics()
		if err != nil {
			log.Printf("could not get database metrics: %s\n", err)
		}
		err = nc.Publish("metrics.postgres", jsonObj)
		if err != nil {
			log.Printf("could not publish database metrics: %s\n", err)
		}
		time.Sleep(5 * time.Second)
	}
}
