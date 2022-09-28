package rdbms

import (
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/eaciit/toolkit"

	"git.kanosolution.net/kano/dbflex"
)

type RdbmsCursor interface {
	Serialize(interface{}) error
	Values() []interface{}
	ValuesPtr() []interface{}

	ColumnNames() []string
	ColumnTypes() []string
	SetColumnNames(names []string)
	SetColumnTypes(types []string)
	PrepareForScan() error
}

type Cursor struct {
	dbflex.CursorBase
	fetcher *sql.Rows
	//dest      []interface{}
	columns   []string
	types     []string
	values    []interface{}
	valuesPtr []interface{}
	//m         toolkit.M

	_this dbflex.ICursor

	query RdbmsQuery
	//dataTypeList toolkit.M

	isPrepared bool
}

func (c *Cursor) Reset() error {
	c.fetcher = nil
	c.isPrepared = false
	//c.dest = []interface{}{}
	return nil
}

func (c *Cursor) SetFetcher(r *sql.Rows) error {
	c.fetcher = r
	err := c.PrepareForScan()
	if err != nil {
		return err
	}

	return nil
}

func (c *Cursor) SetThis(ic dbflex.ICursor) dbflex.ICursor {
	c._this = ic
	return ic
}

func (c *Cursor) this() dbflex.ICursor {
	return c._this
}

func (c *Cursor) ColumnNames() []string {
	return c.columns
}

func (c *Cursor) SetColumnNames(names []string) {
	c.columns = names
}

func (c *Cursor) ColumnTypes() []string {
	return c.types
}

func (c *Cursor) SetColumnTypes(types []string) {
	c.types = types
}

func (c *Cursor) PrepareForScan() error {
	if e := c.Error(); e != nil {
		return e
	}

	c.values = []interface{}{}
	c.valuesPtr = []interface{}{}
	if c.fetcher == nil {
		return fmt.Errorf("scan failed, fetcher is not available")
	}

	names, err := c.fetcher.Columns()
	if err != nil {
		return fmt.Errorf("scan failed. %s", err.Error())
	}
	if len(names) == 0 {
		return fmt.Errorf("scan failed, no columns metadata is available")
	}

	ctypes, err := c.fetcher.ColumnTypes()
	if err != nil {
		return fmt.Errorf("scan failed. %s", err.Error())
	}

	sqlTypes := []string{}
	values := []interface{}{}
	for _, ct := range ctypes {
		typename := strings.ToLower(ct.DatabaseTypeName())
		if strings.HasPrefix(typename, "int") {
			sqlTypes = append(sqlTypes, "int")
			//values = append(values, int(0))
		} else if strings.HasPrefix(typename, "dec") || strings.HasPrefix(typename, "float") ||
			strings.HasPrefix(typename, "number") || strings.HasPrefix(typename, "num") {
			sqlTypes = append(sqlTypes, "float64")
			//values = append(values, float64(0))
		} else if strings.HasPrefix(typename, "date") || strings.HasPrefix(typename, "time") {
			sqlTypes = append(sqlTypes, "time.Time")
			//values = append(values, time.Time{})
		} else if strings.HasPrefix(typename, "json") {
			sqlTypes = append(sqlTypes, "json")
		} else {
			sqlTypes = append(sqlTypes, "string")
			//values = append(values, "")
		}
		values = append(values, []byte{})
		//dbflex.Logger().Debugf("%s | %s | %s", names[idx], typename, sqlTypes[idx])
	}

	ptrs := []interface{}{}
	for idx, _ := range values {
		ptrs = append(ptrs, &values[idx])
	}

	c.values = values
	c.valuesPtr = ptrs
	c.columns = names
	c.types = sqlTypes
	//fmt.Println("[debug] values:", toolkit.JsonString(c.values))
	//fmt.Println("[debug] values ptr:", toolkit.JsonString(c.valuesPtr))

	c.isPrepared = true
	return nil
}

func (c *Cursor) Scan() error {
	if !c.isPrepared {
		if err := c.PrepareForScan(); err != nil {
			return err
		}
	}

	if c.Error() != nil {
		return c.Error()
	}

	if c.fetcher == nil {
		return toolkit.Error("cursor is not valid, no fetcher object specified")
	}

	if !c.fetcher.Next() {
		return dbflex.EOF
	}

	return c.fetcher.Scan(c.valuesPtr...)
}

func (c *Cursor) Values() []interface{} {
	return c.values
}

func (c *Cursor) SetValues(v []interface{}) {
	c.values = v
}

func (c *Cursor) ValuesPtr() []interface{} {
	return c.valuesPtr
}

func (c *Cursor) SetValuesPtr(ptrs []interface{}) {
	c.valuesPtr = ptrs
}

func (c *Cursor) Serialize(dest interface{}) error {
	var err error
	m := toolkit.M{}
	toolkit.Serde(dest, &m, "")

	columnNames := c.ColumnNames()
	sqlTypes := c.ColumnTypes()
	//fmt.Println("\n[debug] values:", toolkit.JsonString(c.values))
	//fmt.Println("\n[debug] values Ptr:", toolkit.JsonString(c.valuesPtr))
	for idx, value := range c.Values() {
		name := columnNames[idx]
		ft := sqlTypes[idx]

		v, ok := value.([]byte)
		if ok {
			switch ft {
			case "int":
				m.Set(name, toolkit.ToInt(string(v), toolkit.RoundingAuto))

			case "float64":
				m.Set(name, toolkit.ToFloat64(string(v), 4, toolkit.RoundingAuto))

			case "time.Time":
				if dt, err := time.Parse(time.RFC3339, string(v)); err == nil {
					m.Set(name, dt)
				} else {
					dt = toolkit.String2Date(string(v), TimeFormat())
					m.Set(name, dt)
				}
			case "json":
				var out interface{}
				if err := toolkit.Unjson(v, &out); err == nil {
					if isSlice := toolkit.IsSlice(out); (isSlice && toolkit.SliceItem(out, 0) != nil) || (!isSlice && out != nil) {
						m.Set(name, out)
					}
				}
			default:
				m.Set(name, string(v))
			}
			//dbflex.Logger().Debugf("%s (%s) = %s\n", name, ft, v)
		} else {
			m.Set(name, value)
			//dbflex.Logger().Debugf("%s = %s\n", name, value)
		}
	}

	err = toolkit.Serde(m, dest, "")
	if err != nil {
		return toolkit.Error(err.Error() + toolkit.Sprintf(" object: %s", toolkit.JsonString(m)))
	}
	return nil
}

func (c *Cursor) Fetch(obj interface{}) dbflex.ICursor {
	err := c.Scan()
	if err != nil {
		c.SetError(err)
		return c
	}

	if err = c.this().(RdbmsCursor).Serialize(obj); err != nil {
		c.SetError(err)
		return c
	}

	return c
}

func (c *Cursor) Fetchs(obj interface{}, n int) dbflex.ICursor {
	var err error
	i := 0
	loop := true
	ms := []toolkit.M{}
	for loop {
		err = c.Scan()
		if err != nil {
			if err == dbflex.EOF {
				loop = false
				err = nil
			} else {
				c.SetError(err)
				return c
			}
		} else {
			mobj := toolkit.M{}
			err = c.this().(RdbmsCursor).Serialize(&mobj)
			if err != nil {
				c.SetError(err)
				return c
			}
			ms = append(ms, mobj)
			i++
			if i >= n && n != 0 {
				//fmt.Println("data", i)
				loop = false
			}
		}
	}

	err = toolkit.Serde(ms, obj, "")
	if err != nil {
		c.SetError(err)
		return c
	}
	return c
}

func (c *Cursor) Close() error {
	var e error
	if c != nil {
		e = c.Error()
		if c.fetcher != nil {
			c.fetcher.Close()
		}
	}
	return e
}
