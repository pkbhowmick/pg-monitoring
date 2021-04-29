package model

type Model struct {
	Statements []Statement `json:"statements"`
	Databases  []Database  `json:"databases"`
}

type Statement struct {
	UserOID   int     `json:"user_oid"`
	DBOID     int     `json:"db_oid"`
	QueryID   int64   `json:"query_id"`
	Query     string  `json:"query"`
	Calls     int64   `json:"calls"`
	TotalTime float64 `json:"total_time"`
	MinTime   float64 `json:"min_time"`
	MaxTime   float64 `json:"max_time"`
}

type Database struct {
	OID           int    `json:"oid"`
	Name          string `json:"name"`
	DatDBA        int    `json:"dat_dba"`
	DatTableSpace int    `json:"dat_table_space"`
	NumBackends   int    `json:"num_backends"`
}
