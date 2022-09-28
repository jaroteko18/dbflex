package dbflex

import (
	"net/url"

	"github.com/eaciit/toolkit"
)

var drivers map[string]func(*ServerInfo) IConnection

// ObjTypeEnum is
type ObjTypeEnum string

const (
	// ObjTypeTable is enumeration of object type table
	ObjTypeTable ObjTypeEnum = "table"
	// ObjTypeView is enumeration of object type view
	ObjTypeView ObjTypeEnum = "view"
	// ObjTypeProcedure is enumeration of object type precedure
	ObjTypeProcedure ObjTypeEnum = "procedure"
	// ObjTypeAll is enumeration of object type all
	ObjTypeAll ObjTypeEnum = "allobject"

	// StateConnected is enumeration of connected state
	StateConnected string = "connected"
	// StateUnknown is enumeration of unknown state
	StateUnknown = ""
)

// IConnection provides interface for database connection
type IConnection interface {
	Connect() error
	State() string
	Close()

	Prepare(ICommand) (IQuery, error)
	Execute(ICommand, toolkit.M) (interface{}, error)
	Cursor(ICommand, toolkit.M) ICursor

	NewQuery() IQuery
	ObjectNames(ObjTypeEnum) []string
	ValidateTable(interface{}, bool) error
	DropTable(string) error
	HasTable(string) bool
	EnsureTable(string, []string, interface{}) error

	BeginTx() error
	Commit() error
	RollBack() error
	SupportTx() bool
	IsTx() bool

	SetThis(IConnection) IConnection
	This() IConnection

	SetFieldNameTag(string)
	FieldNameTag() string

	SetKeyNameTag(string)
	KeyNameTag() string
}

// ConnectionBase is base class to implement IConnection interface
type ConnectionBase struct {
	ServerInfo

	_this IConnection
	_isTx bool

	fieldNameTag string
	keyNameTag   string
}

// SetThis is setter for this
func (b *ConnectionBase) SetThis(t IConnection) IConnection {
	b._this = t
	return t
}

// This is getter for this
func (b *ConnectionBase) This() IConnection {
	if b._this == nil {
		return b
	}

	return b._this
}

// BeginTx to start a transaction
func (b *ConnectionBase) BeginTx() error {
	b._isTx = true
	return nil
}

// Commit commit a transaction
func (b *ConnectionBase) Commit() error {
	b._isTx = false
	return nil
}

// RollBack cancel transaction and rolling back data to their state before it is being trx-ed
func (b *ConnectionBase) RollBack() error {
	b._isTx = false
	return nil
}

// IsTx check if a connection is in a tx mode or not
func (b *ConnectionBase) IsTx() bool {
	return false
}

// SupportTx is this connection supporting tx mode
func (b *ConnectionBase) SupportTx() bool {
	return false
}

// SetFieldNameTag setter for fieldNameTag
func (b *ConnectionBase) SetFieldNameTag(name string) {
	b.fieldNameTag = name
}

// FieldNameTag getter for fieldNameTag
func (b *ConnectionBase) FieldNameTag() string {
	if b.fieldNameTag == "" {
		b.fieldNameTag = "json"
	}
	return b.fieldNameTag
}

// SetKeyNameTag setter for fieldNameTag
func (b *ConnectionBase) SetKeyNameTag(name string) {
	b.keyNameTag = name
}

// KeyNameTag getter for fieldNameTag
func (b *ConnectionBase) KeyNameTag() string {
	if b.keyNameTag == "" {
		b.keyNameTag = "key"
	}
	return b.keyNameTag
}

// Connect establish connection
// In this base struct this method is not yed implemented
func (b *ConnectionBase) Connect() error {
	return toolkit.Error("Connect method is not yet implemented. It should be called from a driver connection object")
}

// State getter for state
// In this base struct we assume state is always unknown, so driver should implement this method
func (b *ConnectionBase) State() string {
	return StateUnknown
}

// Close closing the connection
// In this base struct this method is not yet implemented
func (b *ConnectionBase) Close() {
	// Not yet implemented
}

// NewQuery create new query
// In this base struct this method is not yet implemented
func (b *ConnectionBase) NewQuery() IQuery {
	return nil
}

// ObjectNames return
// In this base struct this method is not yet implemented
func (b *ConnectionBase) ObjectNames(ot ObjTypeEnum) []string {
	return []string{}
}

// ValidateTable in this base struct this method is not yet implemented
func (b *ConnectionBase) ValidateTable(obj interface{}, autoUpdate bool) error {
	return toolkit.Errorf("ValidateSchema is not yet implemented")
}

// DropTable in this base struct this method is not yet implemented
func (b *ConnectionBase) DropTable(name string) error {
	return toolkit.Errorf("DropTable is not yet implemented")
}

// Prepare preparing the given command to a query
func (b *ConnectionBase) Prepare(cmd ICommand) (IQuery, error) {
	var dbCmd interface{}

	if b.This().State() != StateConnected {
		return nil, toolkit.Errorf("no valid connection")
	}

	q := b.This().NewQuery()
	q.SetCommand(cmd)
	err := buildGroupedQueryItems(cmd, q)
	if err == nil {
		dbCmd, err = q.This().BuildCommand()
	}

	if err != nil {
		return nil, toolkit.Errorf("unable to parse command. %s", err)
	}
	q.SetConfig(ConfigKeyCommand, dbCmd)
	return q, nil
}

// Execute given command and M data
func (b *ConnectionBase) Execute(c ICommand, m toolkit.M) (interface{}, error) {
	q, err := b.Prepare(c)
	if err != nil {
		return nil, toolkit.Errorf("unable to prepare query. %s", err.Error())
	}
	q.SetConnection(b.This())
	return q.Execute(m)
}

// Cursor return the cursor of given command and M data
func (b *ConnectionBase) Cursor(c ICommand, m toolkit.M) ICursor {
	q, err := b.Prepare(c)
	if err != nil {
		//return nil, toolkit.Errorf("usnable to prepare query. %s", err.Error())
		cursor := new(CursorBase)
		cursor.SetError(toolkit.Errorf("unable to prepare query. %s", err.Error()))
		return cursor
	}
	cursor := q.Cursor(m)
	cursor.SetConnection(b.This())
	return cursor
}

// ServerInfo hold the server information data
type ServerInfo struct {
	Host, User, Password, Database string
	Config                         toolkit.M
}

// RegisterDriver register a driver for given name and Connection function
func RegisterDriver(name string, fn func(*ServerInfo) IConnection) {
	if drivers == nil {
		drivers = map[string]func(*ServerInfo) IConnection{}
	}
	drivers[name] = fn
}

// NewConnection
func NewConnection(schema, host, dbname, user, password string, config toolkit.M) (IConnection, error) {
	driver := schema
	if fn, ok := drivers[driver]; ok {
		si := new(ServerInfo)
		si.Host = host
		if dbname != "" {
			si.Database = dbname
		}
		if config != nil {
			si.Config = config
		} else {
			si.Config = toolkit.M{}
		}
		if config != nil {
			for k, v := range config {
				si.Config.Set(k, v)
			}
		}
		if user != "" || password != "" {
			si.User = user
			si.Password = password
		}
		return fn(si), nil
	}
	return nil, toolkit.Errorf("driver %s is unknown", driver)
}

// EnsureTable ensure table is exist with respective structure
func (b *ConnectionBase) EnsureTable(name string, keys []string, obj interface{}) error {
	return toolkit.Error("EnsureTable method is not yet implemented. It should be called from a driver connection object")
}

// HasTable check if table exist
func (b *ConnectionBase) HasTable(name string) bool {
	cmd := From(name).Select().Take(1)
	cs := b.This().Cursor(cmd, nil)
	defer cs.Close()
	return cs.Error() == nil
}

// NewConnectionFromConfig in this base struct this method is not yet implemented
func NewConnectionFromConfig(driver, path, name string) IConnection {
	return nil
}

// NewConnectionFromURI create new connection from given uri,
// This method will also determine which driver to use for given uri
// Example URI: json://localhost/Users/sugab/sandbox/go/src/git.kanosolution.net/kano/dbflex/data?extension=json
func NewConnectionFromURI(uri string, config toolkit.M) (IConnection, error) {
	u, e := url.Parse(uri)
	if e != nil {
		return nil, e
	}

	driver := u.Scheme
	if fn, ok := drivers[driver]; ok {
		si := new(ServerInfo)
		si.Host = u.Host
		if len(u.Path) > 1 {
			si.Database = u.Path[1:]
		}
		if config != nil {
			si.Config = config
		} else {
			si.Config = toolkit.M{}
		}
		if u.RawQuery != "" {
			mq, e := url.ParseQuery(u.RawQuery)
			if e == nil {
				for k, v := range mq {
					si.Config.Set(k, v[0])
				}
			}
		}
		if u.User != nil {
			si.User = u.User.Username()
			si.Password, _ = u.User.Password()
		}
		return fn(si), nil
	}
	return nil, toolkit.Errorf("driver %s is unknown", driver)
}

// From set tableName
func From(tableName string) ICommand {
	return new(CommandBase).From(tableName)
}

// SQL  set tableName
func SQL(command string) ICommand {
	return new(CommandBase).SQL(command)
}
