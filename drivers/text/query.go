package text

import (
	"bufio"
	"context"
	"encoding/csv"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"

	"github.com/theckman/go-flock"

	"git.kanosolution.net/kano/dbflex"
	"github.com/eaciit/toolkit"
)

// Query is
type Query struct {
	dbflex.QueryBase

	textObjectSetting *Config
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
	c.openFile()
	c.textObjectSetting = q.textObjectSetting
	return c
}

// Execute the query with its configuration
func (q *Query) Execute(parm toolkit.M) (interface{}, error) {
	cfg := q.textObjectSetting
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

	var file *os.File
	//-- if save, insert, update and delete. create the file
	if (cmdType == dbflex.QueryInsert || cmdType == dbflex.QuerySave || cmdType == dbflex.QueryUpdate || cmdType == dbflex.QueryDelete) && !fileExist {
		file, err = os.Create(filePath)
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

	if fileExist {
		file, err = os.OpenFile(filePath, os.O_APPEND|os.O_RDWR, os.ModeAppend)
		if err != nil {
			return err, toolkit.Errorf("unable to open file %s. %s", filePath, err.Error())
		}
	}

	defer func() {
		fileLock.Unlock()
		file.Close()
		q.Connection().(*Connection).Unlock()
	}()

	// === Update block
	update := func() (int, error) {
		data, hasData := parm["data"]
		if !hasData {
			return 0, toolkit.Errorf("update fail, no data")
		}

		// Keep track of how many rows are updated
		updatedCount := 0

		// Since we cannot add / append text except at the end of the file
		// We need to move updated data and non updated data to temporary file, and the replace existing file with the temporary file
		tmpFile, err := func() (string, error) {
			var tempFile *os.File
			// Randomize temporary file name
			tempFileName := filePath + "_temp_" + toolkit.RandomString(32)
			if tempFile, err = os.Create(tempFileName); err != nil {
				return "", toolkit.Errorf("unable to create temp file. %s", err.Error())
			}
			defer tempFile.Close()

			reader := bufio.NewScanner(file)

			read := -1
			header := []string{}
			for reader.Scan() {
				txt := reader.Text()
				if read < 0 {
					// If first line read it as header
					header = strings.Split(txt, string(cfg.Delimeter))
					tempFile.WriteString(txt + "\n")
					read++
					continue
				}

				// Check if the old data is match with give filter
				ok, err := isIncluded(strings.Split(txt, string(cfg.Delimeter)), header, filter)
				if err != nil {
					return "", err
				}

				if ok {
					// If old data match with given filter
					// Convert the updated fields and its value to M
					m, err := objToM(data)
					if err != nil {
						return "", err
					}

					// Split the old data text to array of string
					oldData := strings.Split(txt, string(cfg.Delimeter))
					// Create place holder for updated data
					updatedData := []string{}
					for i, h := range header {
						match := false
						for k, v := range m {
							// Check old data header with updated fields name
							if strings.ToLower(h) == strings.ToLower(k) {
								// If match append the new value to updated data
								updatedData = append(updatedData, interfaceToText(v, h, cfg))
								match = true
								break
							}
						}

						// If not match then that means that field is left un updated
						if !match {
							// So append the old value to updated data
							updatedData = append(updatedData, oldData[i])
						}
					}

					// Write the updated data to temporary file
					tempFile.WriteString(strings.Join(updatedData, string(cfg.Delimeter)) + "\n")
					// Add the counter
					updatedCount++
				} else {
					// If data is not match with given filter write the old one
					tempFile.WriteString(txt + "\n")
				}
			}
			// Sync the file
			tempFile.Sync()

			return tempFileName, nil
		}()

		if err != nil {
			return updatedCount, err
		}

		// Replace original file with the temporary file
		fp, _ := q.filePath()

		// If there is no update has been made
		if updatedCount == 0 {
			// remove the tempFile
			os.Remove(tmpFile)
		} else {
			// else, rename it and overwrite the old file
			os.Rename(tmpFile, fp)
		}

		return updatedCount, nil
	}
	// === End of update block

	// === Insert block
	insert := func() error {
		written := false
		data, hasData := parm["data"]
		if !hasData {
			return toolkit.Errorf("insert fail, no data")
		}

		singleData := data
		if reflect.TypeOf(data).Kind() == reflect.Slice {
			vd := reflect.ValueOf(data)
			if vd.Len() == 0 {
				return nil
			}
			singleData = vd.Index(0).Interface()
		}

		reader := csv.NewReader(file)
		reader.Comma = cfg.Delimeter
		header, err := reader.Read()
		if err == io.EOF {
			// If end of file then the file is empty, and should add data header first
			header = objHeader(singleData)
			_, err = file.WriteString(strings.Join(header, string(cfg.Delimeter)) + "\n")
			if err != nil {
				return toolkit.Errorf("unable to write to text file %s. %s", filePath, err.Error())
			}
		} else if cfg.WriteMode == ModeLoose {
			combinedHeader := combineHeader(header, objHeader(singleData))

			if len(combinedHeader) > len(header) {
				// Rewrite header
				// Reset file offset back to the begining of the file
				file.Seek(0, 0)

				// Generate addition that we need to add for each exsiting data
				addition := ""
				for i := 0; i < len(combinedHeader)-len(header); i++ {
					addition += string(cfg.Delimeter)
				}

				// Since we cannot add / append text except at the end of the file
				// We need to move new header and data to temporary file, and the replace existing file with the temporary file
				tmpFile, err := func() (string, error) {
					var tempFile *os.File
					// Ranomize file name
					tempFileName := filePath + "_temp_" + toolkit.RandomString(32)
					if tempFile, err = os.Create(tempFileName); err != nil {
						return "", toolkit.Errorf("unable to create temp file. %s", err.Error())
					}
					defer tempFile.Close()

					reader := bufio.NewScanner(file)
					read := -1

					for reader.Scan() {
						if read < 0 {
							// Don't include first line of the file, use the new header instead
							tempFile.WriteString(strings.Join(combinedHeader, string(cfg.Delimeter)) + "\n")
							read++
							continue
						}

						// Write the existing data with the addition
						tempFile.WriteString(reader.Text() + addition + "\n")
					}

					var textDatas []string
					if reflect.TypeOf(data).Kind() == reflect.Slice {
						d := reflect.ValueOf(data)
						textDatas = make([]string, d.Len())

						for i := 0; i < d.Len(); i++ {
							txt, err := objToText(d.Index(i).Interface(), combinedHeader, cfg)
							if err != nil {
								return "", toolkit.Errorf("error serializing data into text. %s", err.Error())
							}

							textDatas[i] = txt
						}
					} else {
						// Convert new data to text
						txt, err := objToText(data, combinedHeader, cfg)
						if err != nil {
							return "", toolkit.Errorf("error serializing data into text. %s", err.Error())
						}

						textDatas = []string{txt}
					}

					for _, td := range textDatas {
						// Write it to the file
						_, err = tempFile.WriteString(td + "\n")
						if err != nil {
							return "", toolkit.Errorf("unable to write to text file %s. %s", filePath, err.Error())
						}
					}

					// Sync the file
					tempFile.Sync()
					if err != nil {
						return "", toolkit.Errorf("unable to write to text file %s. %s", filePath, err.Error())
					}

					// Mark that the new data is already written to the file
					written = true

					return tempFileName, nil
				}()

				if err != nil {
					return err
				}

				//-- delete original file and rename tmpfile to original file
				fp, _ := q.filePath()
				os.Rename(tmpFile, fp)
			}
		}

		if !written {
			// If new data is not yet written then write it
			var textDatas []string
			if reflect.TypeOf(data).Kind() == reflect.Slice {
				d := reflect.ValueOf(data)
				textDatas = make([]string, d.Len())

				for i := 0; i < d.Len(); i++ {
					// Convert new data to text
					txt, err := objToText(d.Index(i).Interface(), header, cfg)
					if err != nil {
						return toolkit.Errorf("error serializing data into text. %s", err.Error())
					}

					textDatas[i] = txt
				}
			} else {
				// Convert new data to text
				txt, err := objToText(data, header, cfg)
				if err != nil {
					return toolkit.Errorf("error serializing data into text. %s", err.Error())
				}

				textDatas = []string{txt}
			}

			for _, td := range textDatas {
				// Write it to the file
				_, err = file.WriteString(td + "\n")
				if err != nil {
					return toolkit.Errorf("unable to write to text file %s. %s", filePath, err.Error())
				}
			}

			// Sync the file
			err = file.Sync()
			if err != nil {
				return toolkit.Errorf("unable to write to text file %s. %s", filePath, err.Error())
			}
		}

		return nil
	}
	// === End of insert block

	switch cmdType {
	case dbflex.QuerySelect:
		return nil, toolkit.Errorf("select command should use cursor instead of execute")

	case dbflex.QuerySave:
		// Check if data is exist
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
			// Run update command
			count, err := update()
			if err != nil {
				return nil, err
			}

			// If no update has beed made, that means the _id doesn't match any data
			if count == 0 {
				// Then insert it
				// But first reset the file cursor
				file.Seek(0, 0)
				return nil, insert()
			}
		} else {
			// If not insert it
			return nil, insert()
		}

	case dbflex.QueryInsert:
		return nil, insert()

	case dbflex.QueryUpdate:
		_, err = update()
		return nil, err

	case dbflex.QueryDelete:
		// If there is no filter at all then it means delete all data
		deleteAll := where == nil
		if deleteAll {
			// Truncate the file
			err = file.Truncate(0)
			if err != nil {
				return nil, toolkit.Errorf("unable to truncated %s", err.Error())
			}

			// Reset the file offset
			file.Seek(0, 0)
		} else {
			// Else then delete data that only match the filter
			// Since we cannot add / append text except at the end of the file
			// We need to move undeleted data to temporary file, and the replace existing file with the temporary file
			tmpFile, err := func() (string, error) {
				var tempFile *os.File
				// Randomize temporary file name
				tempFileName := filePath + "_temp_" + toolkit.RandomString(32)
				if tempFile, err = os.Create(tempFileName); err != nil {
					return "", toolkit.Errorf("unable to create temp file. %s", err.Error())
				}
				defer tempFile.Close()

				reader := bufio.NewScanner(file)

				read := -1
				header := []string{}
				for reader.Scan() {
					txt := reader.Text()
					if read < 0 {
						// If it's the first line then read it as header
						header = strings.Split(txt, string(cfg.Delimeter))
						tempFile.WriteString(txt + "\n")
						read++
						continue
					}

					// Check if the data is match with the given filter
					ok, err := isIncluded(strings.Split(txt, string(cfg.Delimeter)), header, filter)
					if err != nil {
						return "", err
					}

					// If the data is not match with the given filter
					// Then write it into the temporary file
					if !ok {
						tempFile.WriteString(txt + "\n")
					}
				}
				// Sync the file
				tempFile.Sync()

				return tempFileName, nil
			}()

			if err != nil {
				return nil, err
			}

			// Replace original file with the temporary file
			fp, _ := q.filePath()
			os.Rename(tmpFile, fp)
		}

	default:
		return nil, toolkit.Errorf("unknown command: %s", cmdType)
	}

	return nil, nil
}
