package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"github.com/pkbhowmick/pg-monitoring/model"
	"github.com/pkbhowmick/pg-monitoring/pkg/database"
	"log"
	"time"

	"github.com/tidwall/pretty"
)


const (
	DBConnectionConfig = "host=/var/run/postgresql port=5432 user=pulak sslmode=disable application_name=pgmetrics lock_timeout=50 statement_timeout=5000"
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
		return nil,err
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


func main()  {
	connConfig := database.GetDefaultCollectConfig()
	db, err := database.GetDBConnection(DBConnectionConfig,connConfig)
	if err != nil {
		log.Fatalln(err)
	}
	defer db.Close()

	statements, err := GetStatements(db)
	if err != nil {
		log.Fatalln(err)
	}
	data, err := json.Marshal(statements)
	if err != nil {
		log.Fatalln(err)
	}
	jsonStr := string(pretty.Pretty(data))
	fmt.Println(jsonStr)
}