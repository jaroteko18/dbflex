package rdbms

import (
	"bytes"
	"fmt"
	"reflect"
	"strings"
	"time"

	"text/template"

	"git.kanosolution.net/kano/dbflex"
	"github.com/eaciit/toolkit"
)

type RdbmsQuery interface {
	Templates() map[string]string
	ValueToSQlValue(v interface{}) string
}

type Query struct {
	dbflex.QueryBase
}

func (q *Query) Templates() map[string]string {
	return map[string]string{
		string(dbflex.QuerySelect): "SELECT {{.FIELDS}} FROM {{." + dbflex.ConfigKeyTableName + "}} " +
			"{{." + dbflex.QueryWhere + "}} " +
			"{{." + dbflex.QueryOrder + "}} " +
			"{{." + dbflex.QueryGroup + "}} " +
			"{{." + dbflex.QueryTake + "}} " +
			"{{." + dbflex.QuerySkip + "}}",
		//dbflex.QueryWhere: "{{." + dbflex.QueryWhere + "}}",
		dbflex.QueryTake:  "LIMIT {{." + dbflex.QueryTake + "}}",
		dbflex.QuerySkip:  "OFFSET {{." + dbflex.QuerySkip + "}}",
		dbflex.QueryGroup: "{{." + dbflex.QueryGroup + "}}",
		dbflex.QueryOrder: "ORDER BY {{." + dbflex.QueryOrder + "}}",
		dbflex.QueryInsert: "INSERT INTO {{." + dbflex.ConfigKeyTableName + "}} " +
			"({{.FIELDS}}) VALUES ({{.VALUES}})",
		dbflex.QueryUpdate: "UPDATE {{." + dbflex.ConfigKeyTableName + "}} " +
			"SET {{.FIELDVALUES}} {{." + dbflex.QueryWhere + "}}",
		dbflex.QueryDelete: "DELETE FROM {{." + dbflex.ConfigKeyTableName + "}} " +
			"{{." + dbflex.QueryWhere + "}}",
		dbflex.AggrMax:         "MAX({{.FIELD}})",
		dbflex.AggrMin:         "MIN({{.FIELD}})",
		string(dbflex.AggrSum): "SUM({{.FIELD}})",
		dbflex.AggrCount:       "COUNT(*)",
		dbflex.AggrAvg:         "AVG({{.FIELD}})",
	}
}

func (q *Query) buildCommandTemplate(data toolkit.M) (string, error) {
	cmdType, ok := q.Config(dbflex.ConfigKeyCommandType, dbflex.QuerySelect).(string)
	if !ok {
		return "", toolkit.Errorf("Operation is not known. current operation is %s", cmdType)
	}
	commands := q.This().(RdbmsQuery).Templates()
	templateTxt := commands[string(cmdType)]

	if cmdType == dbflex.QuerySelect {
		fields := data.Get("fields", []string{}).([]string)
		orderby := data.Get(dbflex.QueryOrder, "").(string)
		groupby := data.Get(dbflex.QueryGroup, "").(string)
		take := data.Get(dbflex.QueryTake, 0).(int)
		skip := data.Get(dbflex.QuerySkip, 0).(int)

		if len(fields) == 0 {
			data.Set("FIELDS", "*")
		} else {
			data.Set("FIELDS", strings.Join(fields, ","))
		}

		if orderby != "" {
			data.Set(dbflex.QueryOrder,
				executeTemplate(commands[dbflex.QueryOrder],
					toolkit.M{}.Set(dbflex.QueryOrder, orderby)))
		} else {
			data.Set(dbflex.QueryOrder, "")
		}

		if groupby != "" {
			data.Set(dbflex.QueryGroup,
				executeTemplate(commands[dbflex.QueryGroup],
					toolkit.M{}.Set(dbflex.QueryGroup, groupby)))
		} else {
			data.Set(dbflex.QueryGroup, "")
		}

		if take != 0 {
			data.Set(dbflex.QueryTake,
				executeTemplate(commands[dbflex.QueryTake],
					toolkit.M{}.Set(dbflex.QueryTake, take)))
		} else {
			data.Set(dbflex.QueryTake, "")
		}

		if skip != 0 {
			data.Set(dbflex.QuerySkip,
				executeTemplate(commands[dbflex.QuerySkip],
					toolkit.M{}.Set(dbflex.QuerySkip, skip)))
		} else {
			data.Set(dbflex.QuerySkip, "")
		}
	} else if cmdType == dbflex.QueryInsert {
		data.Set("FIELDS", "{{.FIELDS}}").Set("VALUES", "{{.VALUES}}")
	} else if cmdType == dbflex.QueryUpdate {
		data.Set("FIELDVALUES", "{{.FIELDVALUES}}")
	}

	var buff bytes.Buffer
	tmp, err := template.New("main").Parse(templateTxt)
	if err != nil {
		return "", toolkit.Errorf("parsing template error. %s. template: %s", err.Error(), templateTxt)
	}
	err = tmp.Execute(&buff, data)
	if err != nil {
		return "", toolkit.Errorf("execute template error. %s", err.Error())
	}

	return strings.Trim(buff.String(), " "), nil
}

func executeTemplate(templateTxt string, data toolkit.M) string {
	var buff bytes.Buffer
	tmp, err := template.New("main").Parse(templateTxt)
	if err != nil {
		return templateTxt
	}
	err = tmp.Execute(&buff, data)
	if err != nil {
		return templateTxt
	}
	return buff.String()
}

func (q *Query) BuildFilter(f *dbflex.Filter) (interface{}, error) {
	ret := ""

	//panic("not implemented")
	sqlfmts := []string{}
	if f.Value != nil {
		_, _, _, sqlfmts = ParseSQLMetadata(q.This().(RdbmsQuery),
			f.Value)
	}

	switch f.Op {
	case dbflex.OpAnd, dbflex.OpOr:
		txts := []string{}
		for _, item := range f.Items {
			if txt, err := q.BuildFilter(item); err != nil {
				return ret, err
			} else {
				txts = append(txts, txt.(string))
			}
		}
		ret = strings.Join(txts, toolkit.IfEq(f.Op, dbflex.OpAnd, " and ", " or ").(string))

	case dbflex.OpContains:
		contain := ""
		if contains, ok := f.Value.([]string); ok && len(contains) > 0 {
			contain = contains[0]
		}
		ret = f.Field + " like '%" + contain + "%'"
		//ret = strings.Join(rets, " or ")

	case dbflex.OpEndWith:
		ret = f.Field + " like '" + f.Value.(string) + "%'"

	case dbflex.OpEq:
		if strings.HasPrefix(sqlfmts[0], "'") {
			ret = f.Field + " = " + sqlfmts[0]
		} else {
			ret = f.Field + " = " + sqlfmts[0]
		}

	case dbflex.OpGt:
		ret = f.Field + " > " + sqlfmts[0]

	case dbflex.OpGte:
		ret = f.Field + " >= " + sqlfmts[0]

	case dbflex.OpLt:
		ret = f.Field + " < " + sqlfmts[0]

	case dbflex.OpLte:
		ret = f.Field + " <= " + sqlfmts[0]

	case dbflex.OpIn:
		items := []string{}
		for _, v := range sqlfmts {
			item := ""
			if strings.HasPrefix(sqlfmts[0], "'") {
				item = f.Field + " like " + v
			} else {
				item = f.Field + " = " + v
			}
			items = append(items, item)
		}
		ret = strings.Join(items, " or ")

	case dbflex.OpNe:
		if strings.HasPrefix(sqlfmts[0], "'") {
			ret = f.Field + " not like " + sqlfmts[0]
		} else {
			ret = f.Field + " != " + sqlfmts[0]
		}

	case dbflex.OpNin:
		items := []string{}
		for _, v := range sqlfmts {
			item := ""
			if strings.HasPrefix(sqlfmts[0], "'") {
				item = f.Field + " not like " + v
			} else {
				item = f.Field + " != " + v
			}
			items = append(items, item)
		}
		ret = strings.Join(items, " and ")

	case dbflex.OpRange:
		//ret = toolkit.Sprintf("%s >= %s and %s <= %s",
		//	f.Field, sqlfmts[0], f.Field, sqlfmts[1])
		ret = f.Field + " between " + sqlfmts[0] + " and " + sqlfmts[1]
	}

	return ret, nil
}

func (q *Query) BuildCommand() (interface{}, error) {
	parts := q.Config(dbflex.ConfigKeyGroupedQueryItems, dbflex.QueryItems{}).(dbflex.QueryItems)

	ct := q.Config(dbflex.ConfigKeyCommandType, "")
	if ct == dbflex.QuerySQL {
		items := parts[dbflex.QuerySQL]
		return items.Value.(string), nil
	}

	commandData := toolkit.M{}
	tablename := q.Config(dbflex.ConfigKeyTableName, "").(string)
	if len(tablename) == 0 {
		return nil, toolkit.Errorf("Table must be specified")
	}
	commandData.Set(dbflex.ConfigKeyTableName, tablename)

	where := q.Config(dbflex.ConfigKeyWhere, "").(string)
	if strings.Trim(where, " ") == "" {
		commandData.Set(dbflex.QueryWhere, "")
	} else {
		commandData.Set(dbflex.QueryWhere, "WHERE "+where)
	}

	switch ct {
	case dbflex.QuerySelect:
		if items, ok := parts[dbflex.QuerySelect]; ok {
			commandData.Set("fields", items.Value.([]string))
		}
		if items, ok := parts[dbflex.QueryTake]; ok {
			commandData.Set(dbflex.QueryTake, items.Value.(int))
		}
		if items, ok := parts[dbflex.QuerySkip]; ok {
			commandData.Set(dbflex.QuerySkip, items.Value.(int))
		}

		if v, ok := parts[dbflex.QueryOrder]; ok {
			fields := []string{}
			orderfields := v.Value.([]string)
			for _, orderfield := range orderfields {
				if !strings.HasPrefix(orderfield, "-") {
					fields = append(fields, strings.TrimSpace(orderfield))
				} else {
					orderfield = orderfield[1:]
					fields = append(fields, strings.TrimSpace(orderfield)+" desc")
				}
			}
			if len(fields) > 0 {
				commandData.Set(dbflex.QueryOrder, strings.Join(fields, ","))
			}
		} else {
			commandData.Set(dbflex.QueryOrder, "")
		}

		if part, ok := parts[dbflex.QueryAggr]; ok {
			items := part.Value.([]*dbflex.AggrItem)
			fields := []string{}
			for _, item := range items {
				field := ""
				if item.Alias == "" {
					item.Alias = item.Field
				}
				switch item.Op {
				case dbflex.AggrCount:
					field = toolkit.Sprintf("COUNT(*) as %s", item.Alias)

				case dbflex.AggrMax:
					field = toolkit.Sprintf("MAX(%s) as %s", item.Field, item.Alias)

				case dbflex.AggrMin:
					field = toolkit.Sprintf("MIN(%s) as %s", item.Field, item.Alias)

				case dbflex.AggrAvg:
					field = toolkit.Sprintf("AVG(%s) as %s", item.Field, item.Alias)

				case dbflex.AggrSum:
					field = toolkit.Sprintf("SUM(%s) as %s", item.Field, item.Alias)
				}
				if field != "" {
					fields = append(fields, field)
				}
			}
			commandData.Set("fields", fields)
		}

		if groupby, ok := parts[dbflex.QueryGroup]; ok {
			groupbyStr := func() string {
				s := "GROUP BY "
				fields := []string{}
				gs := groupby.Value.([]string)
				for _, g := range gs {
					if strings.TrimSpace(g) != "" {
						fields = append(fields, g)
					}
				}
				if len(fields) == 0 {
					return ""
				}
				return s + strings.Join(fields, ",")
			}()
			commandData.Set(dbflex.QueryGroup, groupbyStr)
		}

	case dbflex.QueryInsert:
		if items, ok := parts[dbflex.QueryInsert]; ok {
			fields := items.Value.([]string)
			if len(fields) > 0 {
				//q.SetConfig("fields", fields)
				//commandData.Set("fields", strings.Join(fields, ","))
			}
		}

	case dbflex.QueryUpdate:
		if items, ok := parts[dbflex.QueryUpdate]; ok {
			fields := items.Value.([]string)
			if len(fields) > 0 {
				q.SetConfig("fields", fields)
			}
		}
	}

	cmdTxt, err := q.buildCommandTemplate(commandData)

	//toolkit.Printfn("Command: %s", cmdTxt)
	//if err == nil {
	//	dbflex.Logger().Infof("Query prepared: %s", cmdTxt)
	//}
	return cmdTxt, err
}

//ParseSQLMetadata returns names, types, values and sql value as string
func ParseSQLMetadata(
	qr RdbmsQuery,
	o interface{}) ([]string, []reflect.Type, []interface{}, []string) {
	names := []string{}
	types := []reflect.Type{}
	values := []interface{}{}
	sqlvalues := []string{}

	if toolkit.IsNil(o) {
		return names, types, values, sqlvalues
	}

	r := reflect.Indirect(reflect.ValueOf(o))
	t := r.Type()

	if r.Kind() == reflect.Struct && t.String() == "time.Time" {
		sqlvalues = append(sqlvalues, qr.ValueToSQlValue(o))
	} else if r.Kind() == reflect.Struct {
		nf := r.NumField()
		for fieldIdx := 0; fieldIdx < nf; fieldIdx++ {
			f := r.Field(fieldIdx)
			ft := t.Field(fieldIdx)
			if !f.CanSet() {
				continue
			}
			v := f.Interface()
			sqlname, ok := ft.Tag.Lookup(toolkit.TagName())
			if sqlname == "-" {
				continue
			}
			if ok && sqlname != "" {
				names = append(names, sqlname)
			} else {
				names = append(names, ft.Name)
			}
			types = append(types, ft.Type)
			values = append(values, v)
			if qr != nil {
				sqlvalues = append(sqlvalues, qr.ValueToSQlValue(v))
			}
		}
	} else if r.Kind() == reflect.Map {
		keys := r.MapKeys()
		for _, k := range keys {
			names = append(names, toolkit.Sprintf("%v", k.Interface()))
			types = append(types, k.Type())

			value := r.MapIndex(k)
			v := value.Interface()
			values = append(values, v)
			if qr != nil {
				sqlvalues = append(sqlvalues, qr.ValueToSQlValue(v))
			}
		}
	} else {
		names = append(names, t.Name())
		types = append(types, t)
		values = append(values, o)
		if qr != nil {
			sqlvalues = append(sqlvalues, qr.ValueToSQlValue(o))
		}
	}

	return names, types, values, sqlvalues
}
func CleanupSQL(s string) string {
	return strings.Replace(s, "'", "''", -1)
}
func (qr *Query) ValueToSQlValue(v interface{}) string {
	switch v.(type) {
	case int, int8, int16, int32, int64:
		return toolkit.Sprintf("%d", v)
	case float32, float64:
		return toolkit.Sprintf("%f", v)
	case time.Time:
		return toolkit.Date2String(v.(time.Time), "'yyyy-MM-dd HH:mm:ss T'")
	case *time.Time:
		dt := v.(*time.Time)
		return toolkit.Date2String(*dt, "'yyyy-MM-dd HH:mm:ss T'")
	case bool:
		if v.(bool) == true {
			return "true"
		}
		return "false"
	default:
		return toolkit.Sprintf("'%s'", CleanupSQL(fmt.Sprintf("%v", toolkit.JsonString(v))))
	}
}
