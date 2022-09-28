package dbflex

const (
	// QuerySelect is SELECT command
	QuerySelect string = "SELECT"
	// QueryFrom is FROM command
	QueryFrom = "FROM"
	// QueryWhere is WHERE command
	QueryWhere = "WHERE"
	// QueryGroup is GROUPBY command
	QueryGroup = "GROUPBY"
	// QueryOrder is ORDERBY command
	QueryOrder = "ORDERBY"
	// QueryInsert is INSERT command
	QueryInsert = "INSERT"
	// QueryUpdate is UPDATE command
	QueryUpdate = "UPDATE"
	// QueryDelete is DELETE command
	QueryDelete = "DELETE"
	// QuerySave is SAVE command
	QuerySave = "SAVE"
	// QueryCommand is COMMAND command
	QueryCommand = "COMMAND"
	// QueryAggr is AGGRREGATION command
	QueryAggr = "AGGR"
	// QueryCustom is CUSTOM command
	QueryCustom = "CUSTOM"
	// QueryTake is TAKE command
	QueryTake = "TAKE"
	// QuerySkip is SKIP command
	QuerySkip = "SKIP"
	// QueryJoin is JOIN command
	QueryJoin = "JOIN"
	// QueryLeftJoin is LEFTJOIN command
	QueryLeftJoin = "LEFTJOIN"
	// QueryRightJoin is RIGHTJOIN command
	QueryRightJoin = "RIGHTJOIN"
	// QuerySQL is SQL command
	QuerySQL = "SQL"
)

// QueryItem holding operation and value
type QueryItem struct {
	Op    string
	Value interface{}
}
