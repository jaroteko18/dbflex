package dbflex

// AggrOp is a string represent enumeration of supported aggregation command
type AggrOp string

const (
	// AggrSum is Summmarize
	AggrSum AggrOp = "$sum"
	// AggrAvg is Average
	AggrAvg = "$avg"
	// AggrMin is Minimum
	AggrMin = "$min"
	// AggrMax is Maximum
	AggrMax = "$max"
	// AggrCount is Count
	AggrCount = "$count"
)

// AggrItem holding the operation, alias, and field
type AggrItem struct {
	Field string
	Op    AggrOp
	Alias string
}

// NewAggrItem create new AggrItem with given parameter
func NewAggrItem(alias string, op AggrOp, field string) *AggrItem {
	a := new(AggrItem)
	if alias == "" {
		alias = field
	}
	a.Alias = alias
	a.Field = field
	a.Op = op
	return a
}

// SetAlias set alias
func (a *AggrItem) SetAlias(alias string) {
	a.Alias = alias
}

// Sum create new aggregation item with AggrSum operation
func Sum(field string) *AggrItem {
	return NewAggrItem(field, AggrSum, field)
}

// Avg create new aggregation item with AggrAvg operation
func Avg(field string) *AggrItem {
	return NewAggrItem(field, AggrAvg, field)
}

// Min create new aggregation item with AggrMin operation
func Min(field string) *AggrItem {
	return NewAggrItem(field, AggrMin, field)
}

// Max create new aggregation item with AggrMax operation
func Max(field string) *AggrItem {
	return NewAggrItem(field, AggrMax, field)
}

// Count create new aggregation item with AggrCount operation
func Count(field string) *AggrItem {
	return NewAggrItem(field, AggrCount, field)
}
