package database

import (
	"context"
	"database/sql"
	"log"
	"os"
	"os/user"
	"strconv"
	"time"

	_ "github.com/lib/pq"
)

// CollectConfig is a bunch of options passed to the Collect() function to
// specify which metrics to collect and how.
type CollectConfig struct {
	// general
	TimeoutSec          uint
	LockTimeoutMillisec uint
	NoSizes             bool

	// collection
	Schema          string
	ExclSchema      string
	Table           string
	ExclTable       string
	SQLLength       uint
	StmtsLimit      uint
	Omit            []string
	OnlyListedDBs   bool
	LogFile         string
	LogDir          string
	LogSpan         uint
	RDSDBIdentifier string
	AllDBs          bool

	// connection
	Host     string
	Port     uint16
	User     string
	Password string
	Role     string
}

// GetDefaultCollectConfig returns a CollectConfig initialized with default values.
// Some environment variables are consulted.
func GetDefaultCollectConfig() CollectConfig {
	cc := CollectConfig{
		// ------------------ general
		TimeoutSec:          5,
		LockTimeoutMillisec: 50,
		//NoSizes: false,

		// ------------------ collection
		//Schema: "",
		//ExclSchema: "",
		//Table: "",
		//ExclTable: "",
		//Omit: nil,
		//OnlyListedDBs: false,
		SQLLength:  500,
		StmtsLimit: 100,
		LogSpan:    5,

		// ------------------ connection
		//Password: "",
	}

	// connection: host
	if h := os.Getenv("PGHOST"); len(h) > 0 {
		cc.Host = h
	} else {
		cc.Host = "/var/run/postgresql"
	}

	// connection: port
	if ps := os.Getenv("PGPORT"); len(ps) > 0 {
		if p, err := strconv.Atoi(ps); err == nil && p > 0 && p < 65536 {
			cc.Port = uint16(p)
		} else {
			cc.Port = 5432
		}
	} else {
		cc.Port = 5432
	}

	// connection: user
	if u := os.Getenv("PGUSER"); len(u) > 0 {
		cc.User = u
	} else if u, err := user.Current(); err == nil && u != nil {
		cc.User = u.Username
	} else {
		cc.User = ""
	}

	return cc
}

func GetDBConnection(connstr string, o CollectConfig) (*sql.DB,error) {
	// connect
	db, err := sql.Open("postgres", connstr)
	if err != nil {
		return nil,err
	}

	// ping
	t := time.Duration(o.TimeoutSec) * time.Second
	ctx, cancel := context.WithTimeout(context.Background(), t)
	defer cancel()
	if err := db.PingContext(ctx); err != nil {
		log.Fatal(err)
	}


	// ensure only 1 conn
	db.SetMaxIdleConns(1)
	db.SetMaxOpenConns(1)

	return db, nil
}