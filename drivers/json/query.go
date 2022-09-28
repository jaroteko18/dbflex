package json

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"git.kanosolution.net/kano/dbflex"
	"github.com/eaciit/toolkit"
	flock "github.com/theckman/go-flock"
)

// Query is
type Query struct {
	dbflex.QueryBase
}

var _ dbflex.IQuery = &Query{}

// BuildFilter passes raw dbflex.Filter to the caller
func (q *Query) BuildFilter(f *dbflex.Filter) (interface{}, error) {
	return f, nil
}

// BuildCommand is not yet implemented
func (q *Query) BuildCommand() (interface{}, error) {
	return nil, nil
}

func (q *Query) filePath() (string, error) {
	conn := q.Connection().(*Connection)
	filename := ""
	tablename := q.Config(dbflex.ConfigKeyTableName, "").(string)

	if tablename == "" {
		return "", toolkit.Errorf("no tablename is specified")
	}

	filename = tablename + "." + conn.extension
	filePath := filepath.Join(conn.dirPath, filename)
	return filePath, nil
}

// Cursor return cursor object for this query
func (q *Query) Cursor(toolkit.M) dbflex.ICursor {
	c := new(Cursor)
	c.SetThis(c)
	c.SetConnection(q.Connection())

	filePath, err := q.filePath()
	if err != nil {
		c.SetError(err)
	}
	if _, err := os.Stat(filePath); err != nil {
		if err == os.ErrNotExist {
			//-- do something here
		} else {
			c.SetError(err)
			return c
		}
	}

	filter := q.Config(dbflex.ConfigKeyWhere, nil)
	if filter != nil {
		c.filter = filter.(*dbflex.Filter)
	}
	c.extra = q.Config(dbflex.ConfigKeyGroupedQueryItems, dbflex.QueryItems{}).(dbflex.QueryItems)

	c.filePath = filePath
	return c
}

// Execute the query with its configuration
func (q *Query) Execute(parm toolkit.M) (interface{}, error) {
	cmdType := q.Config(dbflex.ConfigKeyCommandType, "").(string)
	where := q.Config(dbflex.ConfigKeyWhere, nil)

	var filter *dbflex.Filter
	if where != nil {
		filter = where.(*dbflex.Filter)
	}

	filePath, err := q.filePath()
	if err != nil {
		return nil, err
	}

	fileExist := false
	if _, err = os.Stat(filePath); err == nil {
		fileExist = true
	}

	//-- if save, insert, update and delete. create the file
	if (cmdType == dbflex.QueryInsert || cmdType == dbflex.QuerySave || cmdType == dbflex.QueryUpdate || cmdType == dbflex.QueryDelete) && !fileExist {
		// Since this is json file write empty json array to the newly created file
		file, err := os.Create(filePath)
		file.WriteString("[]")
		file.Close()

		if err != nil {
			return err, toolkit.Errorf("unable to create file %s. %s", filePath, err.Error())
		}
	}

	// Mutex lock to manage multiple query run in a single connection
	q.Connection().(*Connection).Lock()

	// File locking to manage multiple connection open the same file
	// Time out is set to 30 second
	lockCtx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	fileLock := flock.NewFlock(filePath)
	// Try to get exclusive lock every 10ms until time out above
	_, err = fileLock.TryLockContext(lockCtx, 10*time.Millisecond)
	if err != nil {
		return err, toolkit.Errorf("unable to lock file %s. %s", filePath, err.Error())
	}

	defer func() {
		fileLock.Unlock()
		q.Connection().(*Connection).Unlock()
	}()

	// Open file for writing mode
	file, err := os.OpenFile(filePath, os.O_RDWR, os.ModeExclusive)
	if err != nil {
		return nil, err
	}
	// Don't forget to close the file after ;)
	defer file.Close()

	// Insert block
	insert := func() error {
		data, hasData := parm["data"]
		if !hasData {
			return toolkit.Errorf("insert fail, no data")
		}

		datas := []interface{}{}

		decoder := json.NewDecoder(file)
		decoder.Decode(&datas)

		if reflect.TypeOf(data).Kind() == reflect.Slice {
			d := reflect.ValueOf(data)
			nd := make([]interface{}, d.Len())

			for i := 0; i < d.Len(); i++ {
				nd[i] = d.Index(i).Interface()
			}

			datas = append(datas, nd...)
		} else {
			datas = append(datas, data)
		}

		err := writeToJSONFile(datas, file)
		if err != nil {
			return err
		}

		return nil
	}
	// End of insert block

	switch cmdType {
	case dbflex.QuerySelect:
		return nil, toolkit.Errorf("select command should use cursor instead of execute")

	case dbflex.QuerySave:
		data, hasData := parm["data"]
		if !hasData {
			return nil, toolkit.Errorf("update fail, no data")
		}
		// Convert to flat M for easier access
		mData, err := objToM(data)
		if err != nil {
			return nil, err
		}

		// BUG: if the ID is not _id this command will not work
		// Check if _id is exist
		if v, ok := mData["_id"]; ok {
			// If exist, create a filter eq _id
			filter = dbflex.Eq("_id", v)

			// Initiate new decoder from stream
			decoder := json.NewDecoder(file)
			// Read open bracket
			_, err = decoder.Token()
			if err != nil {
				return nil, err
			}

			// Initiate updated datas
			updatedData := []interface{}{}
			updatedCount := 0

			// Check if there is more data
			for decoder.More() {
				ed := toolkit.M{}
				// Decode data one by one
				err := decoder.Decode(&ed)
				if err != nil {
					return nil, err
				}

				// Check if the old data is match with give filter
				ok, err := isIncluded(ed, filter)
				if err != nil {
					return nil, err
				}

				if ok {
					// If old data match with given filter
					// Convert the updated fields and its value to M
					m, err := objToM(data)
					if err != nil {
						return "", err
					}

					for _, h := range ed.Keys() {
						for k, v := range m {
							// Check old data header with updated fields name
							if strings.ToLower(h) == strings.ToLower(k) {
								ed[h] = v
								break
							}
						}
					}

					updatedCount++
				}

				updatedData = append(updatedData, ed)
			}

			if updatedCount == 0 {
				updatedData = append(updatedData, data)
			}

			err = writeToJSONFile(updatedData, file)
			if err != nil {
				return nil, err
			}
		} else {
			return nil, insert()
		}

	case dbflex.QueryInsert:
		return nil, insert()

	case dbflex.QueryUpdate:
		data, hasData := parm["data"]
		if !hasData {
			return nil, toolkit.Errorf("update fail, no data")
		}

		// Initiate new decoder from stream
		decoder := json.NewDecoder(file)
		// Read open bracket
		_, err = decoder.Token()
		if err != nil {
			return nil, err
		}

		// Initiate updated datas
		updatedData := []toolkit.M{}

		// Check if there is more data
		for decoder.More() {
			ed := toolkit.M{}
			// Decode data one by one
			err := decoder.Decode(&ed)
			if err != nil {
				return nil, err
			}

			// Check if the old data is match with give filter
			ok, err := isIncluded(ed, filter)
			if err != nil {
				return nil, err
			}

			if ok {
				// If old data match with given filter
				// Convert the updated fields and its value to M
				m, err := objToM(data)
				if err != nil {
					return "", err
				}

				for _, h := range ed.Keys() {
					for k, v := range m {
						// Check old data header with updated fields name
						if strings.ToLower(h) == strings.ToLower(k) {
							ed[h] = v
							break
						}
					}
				}
			}

			updatedData = append(updatedData, ed)
		}

		err = writeToJSONFile(updatedData, file)
		if err != nil {
			return nil, err
		}

	case dbflex.QueryDelete:
		// If there is no filter at all then it means delete all data
		deleteAll := where == nil
		if deleteAll {
			err := writeToJSONFile([]interface{}{}, file)
			if err != nil {
				return nil, err
			}
		} else {
			// Else then delete data that only match the filter
			// Initiate new decoder from stream
			decoder := json.NewDecoder(file)
			// Read open bracket
			_, err = decoder.Token()
			if err != nil {
				return nil, err
			}

			// Initiate updated datas
			updatedData := []toolkit.M{}

			// Check if there is more data
			for decoder.More() {
				data := toolkit.M{}
				// Decode data one by one
				err := decoder.Decode(&data)
				if err != nil {
					return nil, err
				}

				ok, err := isIncluded(data, filter)
				if err != nil {
					return nil, err
				}

				if !ok {
					updatedData = append(updatedData, data)
				}
			}

			// Read closed bracket
			_, err = decoder.Token()
			if err != nil {
				return nil, err
			}

			err = writeToJSONFile(updatedData, file)
			if err != nil {
				return nil, err
			}
		}

	default:
		return nil, toolkit.Errorf("unknown command: %s", cmdType)
	}

	return nil, nil
}
