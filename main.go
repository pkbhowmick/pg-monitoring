package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/pkbhowmick/pg-monitoring/model"
	"github.com/pkbhowmick/pg-monitoring/pkg/database"
	"github.com/tidwall/pretty"
	"gopkg.in/ini.v1"
)

var (
	cfg                *ini.File
	DBConnectionConfig string
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
		fmt.Println("Error in db collection creation")
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
	servers := cfg.Section("NATS").Key("NATS_URL").String()

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

func PublishEvent(nc *nats.Conn, subject string, data []byte) error {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	for {
		err := nc.Publish(subject, data)
		// _, err := nc.Request(subject, data, 5*time.Second)
		if err == nil {
			cancel()
		} else {
			log.Printf("could not publish database metrics: %s\n", err)
		}

		select {
		case <-ctx.Done():
			if ctx.Err() == context.DeadlineExceeded {
				return errors.New("timeout")
			} else if ctx.Err() == context.Canceled {
				log.Println("successfully published event")
				return nil
			}
		default:
			time.Sleep(time.Second)
		}
	}
}

func main() {
	var err error
	cfg, err = ini.Load("./app.ini")
	if err != nil {
		log.Fatalln(err)
	}

	DBConnectionConfig = cfg.Section("DATABASE").Key("DB_URL").String()

	nc, err := NewConnection()
	if err != nil {
		log.Fatalln(err)
	}

	for {
		jsonObj, err := GetJsonMetrics()
		if err != nil {
			log.Printf("could not get database metrics: %s\n", err)
		}

		err = PublishEvent(nc, "metrics.postgres", jsonObj)
		if err != nil {
			log.Println(err)
		}
		time.Sleep(5 * time.Second)
	}
}
