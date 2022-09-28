package dbflex

// QueryParam is query paramater like Where, Sort, Take, and Skip
type QueryParam struct {
	Where      *Filter
	Sort       []string
	Take, Skip int
	GroupBy    []string
	Select     []string
	Aggregates []*AggrItem
}

// NewQueryParam create new QueryParam
func NewQueryParam() *QueryParam {
	return new(QueryParam)
}

// SetSelect setter for select
func (q *QueryParam) SetSelect(fields ...string) *QueryParam {
	q.Select = fields
	return q
}

// SetWhere setter for Where field
func (q *QueryParam) SetWhere(f *Filter) *QueryParam {
	q.Where = f
	return q
}

// SetSort setter for Sort field
func (q *QueryParam) SetSort(sorts ...string) *QueryParam {
	q.Sort = sorts
	return q
}

// SetTake setter for Take field
func (q *QueryParam) SetTake(take int) *QueryParam {
	q.Take = take
	return q
}

// SetSkip setter for Skip field
func (q *QueryParam) SetSkip(skip int) *QueryParam {
	q.Skip = skip
	return q
}

// SetGroupBy setter for group by
func (q *QueryParam) SetGroupBy(gs ...string) *QueryParam {
	q.GroupBy = gs
	return q
}

// SetAggr setter for aggregates
func (q *QueryParam) SetAggr(aggrs ...*AggrItem) *QueryParam {
	q.Aggregates = aggrs
	return q
}
