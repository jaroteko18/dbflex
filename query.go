package dbflex

import (
	"github.com/eaciit/toolkit"
)

const (
	// ConfigKeyCommand is key config for Command
	ConfigKeyCommand string = "dbfcmd"
	// ConfigKeyCommandType is key config for CommandType
	ConfigKeyCommandType = "dbfcmdtype"
	// ConfigKeyGroupedQueryItems is key config for GroupedQueryItems
	ConfigKeyGroupedQueryItems = "dbfgqis"
	// ConfigKeyWhere is key config for Where
	ConfigKeyWhere = "dbfwhere"
	// ConfigKeyTableName is key config for TableName
	ConfigKeyTableName = "tablenames"
	// ConfigKeyFilter is key config for Filter
	ConfigKeyFilter = "filter"
)

// IQuery is interface abstraction fo all query should be supported by each driver
type IQuery interface {
	SetThis(q IQuery)
	This() IQuery
	BuildFilter(*Filter) (interface{}, error)
	BuildCommand() (interface{}, error)

	Cursor(toolkit.M) ICursor
	Execute(toolkit.M) (interface{}, error)

	SetConfig(string, interface{})
	SetConfigM(toolkit.M)
	Config(string, interface{}) interface{}
	ConfigRef(string, interface{}, interface{})
	DeleteConfig(...string)

	Connection() IConnection
	SetConnection(IConnection)

	SetCommand(ICommand) IQuery
	Command() ICommand
}

// QueryBase is base struct for struct that implement IQuery for easier interface implementation
type QueryBase struct {
	items []*QueryItem

	self        IQuery
	commandType string

	prepared bool
	cmd      ICommand
	conn     IConnection

	config toolkit.M
}

// GroupedQueryItems isc
//type GroupedQueryItems map[string][]*QueryItem

func (q *QueryBase) initConfig() {
	if q.config == nil {
		q.config = toolkit.M{}
	}
}

// Connection getter for conn field
func (q *QueryBase) Connection() IConnection {
	return q.conn
}

// SetConnection setter for conn field
func (q *QueryBase) SetConnection(conn IConnection) {
	q.conn = conn
}

// SetConfig setter for config that accept string and value parameter
func (q *QueryBase) SetConfig(key string, value interface{}) {
	q.initConfig()
	q.config.Set(key, value)
}

// SetConfigM setter for config that accept M
func (q *QueryBase) SetConfigM(in toolkit.M) {
	for k, v := range in {
		q.SetConfig(k, v)
	}
}

// Config getter for config
func (q *QueryBase) Config(key string, def interface{}) interface{} {
	q.initConfig()
	return q.config.Get(key, def)
}

// ConfigRef getter for config that assign the reference to given paramter
func (q *QueryBase) ConfigRef(key string, def, out interface{}) {
	q.initConfig()
	q.config.Get(key, def, out)
}

// DeleteConfig delete config for given keys
func (q *QueryBase) DeleteConfig(deletedkeys ...string) {
	q.initConfig()
	for _, delkey := range deletedkeys {
		delete(q.config, delkey)
	}
}

// BuildCommand not yet implemented in this base struct
func (q *QueryBase) BuildCommand() (interface{}, error) {
	return nil, nil
}

func buildGroupedQueryItems(cmd ICommand, b IQuery) error {
	groupeditems := cmd.Items()

	if fromItem, ok := groupeditems[QueryFrom]; ok {
		b.This().SetConfig(ConfigKeyTableName, fromItem.Value.(string))
	}

	if filter, ok := groupeditems[QueryWhere]; ok {
		translatedFilter, err := b.This().BuildFilter(filter.Value.(*Filter))
		if err != nil {
			return err
		}
		b.This().SetConfig(ConfigKeyWhere, translatedFilter)
		b.This().SetConfig(ConfigKeyFilter, filter.Value.(*Filter))
	}

	if selectItem, ok := groupeditems[QuerySelect]; ok {
		b.This().SetConfig(ConfigKeyCommandType, QuerySelect)
		fields := selectItem.Value.([]string)
		if len(fields) > 0 {
			b.This().SetConfig("fields", fields)
		}
	} else if _, ok := groupeditems[QueryAggr]; ok {
		b.This().SetConfig(ConfigKeyCommandType, QuerySelect)
	} else if insertItem, ok := groupeditems[QueryInsert]; ok {
		b.This().SetConfig(ConfigKeyCommandType, QueryInsert)
		fields := insertItem.Value.([]string)
		if len(fields) > 0 {
			b.This().SetConfig("fields", fields)
		}
	} else if updateItem, ok := groupeditems[QueryUpdate]; ok {
		b.This().SetConfig(ConfigKeyCommandType, QueryUpdate)
		fields := updateItem.Value.([]string)
		if len(fields) > 0 {
			b.This().SetConfig("fields", fields)
		}
	} else if _, ok := groupeditems[QueryDelete]; ok {
		b.This().SetConfig(ConfigKeyCommandType, QueryDelete)
	} else if _, ok = groupeditems[QuerySave]; ok {
		b.This().SetConfig(ConfigKeyCommandType, QuerySave)
	} else if _, ok = groupeditems[QuerySQL]; ok {
		b.This().SetConfig(ConfigKeyCommandType, QuerySQL)
	} else {
		b.This().SetConfig(ConfigKeyCommandType, QueryCommand)
	}
	b.This().SetConfig(ConfigKeyGroupedQueryItems, groupeditems)

	qop := b.Config(ConfigKeyCommandType, "")
	if qop == "" {
		return toolkit.Errorf("unable to build group query items. Invalid QueryOP is defined (%s)", qop)
	}
	return nil
}

// SetThis setter for self
func (q *QueryBase) SetThis(o IQuery) {
	q.self = o
}

// This getter for self
func (q *QueryBase) This() IQuery {
	if q.self == nil {
		return q
	}

	return q.self
}

// BuildFilter not yet implemented in this base struct
func (q *QueryBase) BuildFilter(f *Filter) (interface{}, error) {
	return nil, nil
}

// Cursor not yet implemented in this base struct
func (q *QueryBase) Cursor(in toolkit.M) ICursor {
	c := new(CursorBase)
	c.SetError(toolkit.Error("Cursor is not yet implemented"))
	return c
}

// Execute not yet implemented in this base struct
func (q *QueryBase) Execute(in toolkit.M) (interface{}, error) {
	return nil, toolkit.Error("Execute is not yet implemented")
}

// SetCommand set command
func (q *QueryBase) SetCommand(cmd ICommand) IQuery {
	q.cmd = cmd
	return q.This()
}

// Command get command
func (q *QueryBase) Command() ICommand {
	return q.cmd
}
