package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log"
	"net/http"
	"time"

	"github.com/pkbhowmick/pg-monitoring/pkg/database"
	"github.com/tidwall/pretty"

	"github.com/gorilla/mux"
	"github.com/pkbhowmick/pg-monitoring/model"
)

const (
	DBConnectionConfig = "provide db config here"
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

func GetJsonMetrics(res http.ResponseWriter, req *http.Request) {
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
		log.Fatalln(err)
	}

	model.Databases, err = GetDatabases(db)
	if err != nil {
		log.Fatalln(err)
	}

	model.Tables, err = GetTablesInfo(db)
	if err != nil {
		log.Fatalln(err)
	}

	data, err := json.Marshal(model)
	if err != nil {
		log.Fatalln(err)
	}
	jsonStr := pretty.Pretty(data)
	res.Write(jsonStr)
}

func GetPromMetrics(res http.ResponseWriter, req *http.Request) {
	res.Write([]byte(`Prometheus metrics will come here`))
}

func main() {

	router := mux.NewRouter()

	router.HandleFunc("/json", GetJsonMetrics).Methods(http.MethodGet)

	router.HandleFunc("/metrics", GetPromMetrics).Methods(http.MethodGet)

	server := &http.Server{
		Addr:    ":8099",
		Handler: router,
	}
	log.Println("Server is listening on port 8099")
	log.Fatal(server.ListenAndServe())
}
