package model

type Model struct {
	Statements []Statement `json:"statements"`
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
