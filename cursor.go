package dbflex

import (
	"errors"
	"time"

	"github.com/eaciit/toolkit"
)

// ICursor provides interface for Cursor
type ICursor interface {
	Reset() error
	Fetch(interface{}) ICursor
	Fetchs(interface{}, int) ICursor
	Count() int
	CountAsync() <-chan int
	Close() error
	Error() error
	CloseAfterFetch() bool
	SetCountCommand(ICommand)
	CountCommand() ICommand

	Connection() IConnection
	SetConnection(IConnection)

	ConfigRef(key string, def interface{}, out interface{})
	Set(key string, value interface{})

	SetCloseAfterFetch() ICursor
	AutoClose(time.Duration) ICursor
}

// CursorBase is base sctruct for easier implementation of ICursor
type CursorBase struct {
	err             error
	closeAfterFetch bool

	self         ICursor
	countCommand ICommand
	conn         IConnection

	config toolkit.M
}

// SetError is setter for err
func (b *CursorBase) SetError(err error) ICursor {
	b.err = err
	return b.this()
}

// Error is getter for err
func (b *CursorBase) Error() error {
	return b.err
}

// Connection is getter for conn
func (b *CursorBase) Connection() IConnection {
	return b.conn
}

// ConfigRef assign reference for given key to out parameter
func (b *CursorBase) ConfigRef(key string, def, out interface{}) {
	if b.config == nil {
		b.config = toolkit.M{}
	}
	b.config.GetRef(key, def, out)
}

// Set will set config for given key and value
func (b *CursorBase) Set(key string, value interface{}) {
	if b.config == nil {
		b.config = toolkit.M{}
	}
	b.config.Set(key, value)
}

// SetConnection is setter for conn
func (b *CursorBase) SetConnection(conn IConnection) {
	b.conn = conn
}

func (b *CursorBase) this() ICursor {
	if b.self == nil {
		return b
	}

	return b.self
}

func (b *CursorBase) AutoClose(d time.Duration) ICursor {
	go func() {
		if d > 0 {
			time.Sleep(d)
			b.this().Close()
		}
	}()
	return b.this()
}

// SetThis setter for self
func (b *CursorBase) SetThis(o ICursor) ICursor {
	b.self = o
	return o
}

// Reset is not implemented in this base class
func (b *CursorBase) Reset() error {
	return errors.New("Reset not implemented")
}

// Fetch is not implemented in this base class
func (b *CursorBase) Fetch(interface{}) ICursor {
	return b.this()
}

// Fetchs is not implemented in this base class
func (b *CursorBase) Fetchs(interface{}, int) ICursor {
	return b.this()
}

// Count return count of the record
func (b *CursorBase) Count() int {
	if b.countCommand == nil {
		//fmt.Println("cursor has no count command")
		b.SetError(toolkit.Errorf("cursor has no count command"))
		return 0
	}

	recordcount := struct {
		Count int
	}{}

	if b.conn == nil {
		//fmt.Println("connection object is not initialized")
		b.SetError(toolkit.Errorf("connection object is not defined"))
		return 0
	}

	//err := b.countCommand.Cursor(nil).Fetch(&recordcount)
	cur := b.conn.Cursor(b.CountCommand(), nil)
	if cur.Error() != nil {
		return 0
	}
	defer cur.Close()
	err := cur.Fetch(&recordcount)
	if err != nil {
		//fmt.Printf("unable to get count. %s\n", err.Error())
		b.SetError(toolkit.Errorf("unable to get count. %s", err.Error()))
		return 0
	}

	return recordcount.Count
}

// CountAsync return count of the record but in asynchronus way
func (b *CursorBase) CountAsync() <-chan int {
	out := make(chan int)
	go func(o chan int) {
		o <- b.Count()
	}(out)
	return out
}

// SetCountCommand setter for countCommand
func (b *CursorBase) SetCountCommand(q ICommand) {
	b.countCommand = q
}

// CountCommand getter for countCommand
func (b *CursorBase) CountCommand() ICommand {
	return b.countCommand
}

// SetCloseAfterFetch set closeafterfetch to true
func (b *CursorBase) SetCloseAfterFetch() ICursor {
	b.closeAfterFetch = true
	return b.this()
}

// CloseAfterFetch getter for closeafterfetch
func (b *CursorBase) CloseAfterFetch() bool {
	return b.closeAfterFetch
}

// Close is not implemented in this base class
func (b *CursorBase) Close() error {
	return b.Error()
}

// Serialize is not implemented in this base class
func (b *CursorBase) Serialize(dest interface{}) error {
	return toolkit.Error("Serialize is not yet implemented")
}
