package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/pkbhowmick/pg-monitoring/model"
	"github.com/pkbhowmick/pg-monitoring/pkg/database"

	"github.com/tidwall/pretty"
)

const (
	DBConnectionConfig = "host=/var/run/postgresql port=5432 user=pulak sslmode=disable lock_timeout=50 statement_timeout=5000"
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

func main() {
	connConfig := database.GetDefaultCollectConfig()
	db, err := database.GetDBConnection(DBConnectionConfig, connConfig)
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	var model model.Model

	model.Statements, err = GetStatements(db)
	if err != nil {
		log.Fatalln(err)
	}

	model.Databases, err = GetDatabases(db)
	if err != nil {
		log.Fatalln(err)
	}

	data, err := json.Marshal(model)
	if err != nil {
		log.Fatalln(err)
	}
	jsonStr := string(pretty.Pretty(data))
	fmt.Println(jsonStr)
}
