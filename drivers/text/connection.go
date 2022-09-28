package text

import (
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"

	"github.com/eaciit/toolkit"

	"git.kanosolution.net/kano/dbflex"
)

func init() {
	//=== sample: text://localhost?path=/usr/local/txt
	dbflex.RegisterDriver("text", func(si *dbflex.ServerInfo) dbflex.IConnection {
		c := new(Connection)
		c.ServerInfo = *si
		c.SetThis(c)
		c.SetFieldNameTag("sql")
		return c
	})
}

// Connection is struct that used for this driver to hold configuration needed to make the connection.
// This struct also embeding from dbflex.ConnectionBase and is implementation of dbflex.IConnection
type Connection struct {
	dbflex.ConnectionBase
	sync.Mutex

	dirInfo   os.FileInfo
	dirPath   string
	extension string
	config    *Config
}

var _ dbflex.IConnection = &Connection{}

// Connect will search for directory database from give path, if path is not found or is not a directory will return error
func (c *Connection) Connect() error {
	dirpath := c.Database
	if dirpath == "" {
		return toolkit.Errorf("")
	}

	fi, err := os.Stat(dirpath)
	if err != nil {
		return err
	}

	if fi.IsDir() == false {
		return toolkit.Errorf("%s is not a directory", dirpath)
	}

	c.dirInfo = fi
	c.dirPath = dirpath

	c.extension = c.Config.Get("extension", "").(string)
	c.config = c.Config.Get("text_obj_setting", NewConfig(',')).(*Config)
	return nil
}

// State return the current state of the connection
func (c *Connection) State() string {
	if c.dirInfo != nil {
		return dbflex.StateConnected
	}

	return dbflex.StateUnknown
}

// Close reset all the existing connected directory configuration
func (c *Connection) Close() {
	c.dirInfo = nil
	c.dirPath = ""
}

// NewQuery return new Query with passed configuration needed
func (c *Connection) NewQuery() dbflex.IQuery {
	q := new(Query)
	q.SetThis(q)
	q.SetConnection(c)
	q.textObjectSetting = c.config
	return q
}

// ObjectNames is
func (c *Connection) ObjectNames(dbflex.ObjTypeEnum) []string {
	files, err := ioutil.ReadDir(c.dirPath)
	if err != nil {
		return []string{}
	}

	names := []string{}
	for _, fi := range files {
		name := strings.ToLower(fi.Name())
		if len(c.extension) == 0 {
			names = append(names, name)
		} else {
			if strings.HasSuffix(name, "."+c.extension) {
				names = append(names, name[0:len(name)-len(c.extension)-1])
			}
		}
	}
	return names
}

// ValidateTable not implemented
func (c *Connection) ValidateTable(interface{}, bool) error {
	return nil
}

// DropTable remove the file of given table name
func (c *Connection) DropTable(name string) error {
	filepath := filepath.Join(c.dirPath, name)
	return os.Remove(filepath)
}
