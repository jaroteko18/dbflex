package text

import (
	"bufio"
	"os"
	"reflect"
	"strings"

	"github.com/eaciit/toolkit"

	"git.kanosolution.net/kano/dbflex"
)

// Cursor responsible for converting the text data to given buffer
// For this driver cursor also responsible for filtering, aggregating, grouping, and sorting
type Cursor struct {
	dbflex.CursorBase

	f                 *os.File
	filePath          string
	scanner           *bufio.Scanner
	textObjectSetting *Config
	filter            *dbflex.Filter
	extra             dbflex.QueryItems
}

var _ dbflex.ICursor = &Cursor{}

// Reset is not yet implemented
func (c *Cursor) Reset() error {
	panic("not implemented")
}

// Fetch single data
func (c *Cursor) Fetch(out interface{}) dbflex.ICursor {
	sliceOfT := reflect.SliceOf(reflect.TypeOf(out))
	ptr := reflect.New(sliceOfT)
	ptr.Elem().Set(reflect.MakeSlice(sliceOfT, 0, 0))
	buffer := ptr.Interface()

	err := c.Fetchs(buffer, 1).Error()
	if err != nil {
		return c
	}

	rv := reflect.Indirect(reflect.ValueOf(buffer))

	if rv.Len() > 0 {
		v1 := reflect.Indirect(rv.Index(0))
		reflect.Indirect(reflect.ValueOf(out)).Set(v1)
	} else {
		c.SetError(dbflex.EOF)
		return c
	}

	return c
}

// Fetchs multiple data and require slice as buffer
func (c *Cursor) Fetchs(result interface{}, n int) dbflex.ICursor {
	if c.scanner == nil {
		c.openFile()
	}

	if c.Error() != nil {
		return c
	}

	// Check if there is aggragation and groupby command
	aggrs, hasAggr := c.extra[dbflex.QueryAggr]
	groupby, hasGroup := c.extra[dbflex.QueryGroup]
	sortBy, hasSort := c.extra[dbflex.QueryOrder]
	skip := 0
	take := n

	if items, ok := c.extra[dbflex.QuerySkip]; ok {
		skip = items.Value.(int)
	}

	if items, ok := c.extra[dbflex.QueryTake]; ok {
		take = items.Value.(int)
	}

	loop := true
	header := []string{}
	read := -1
	shouldFetched := 0

	if !hasSort && !hasAggr && !hasGroup {
		read -= skip
		shouldFetched = take
	}

	v := reflect.TypeOf(result).Elem().Elem()
	// Create empty slice of buffer element type
	ivs := reflect.MakeSlice(reflect.SliceOf(v), 0, 0)

	for c.scanner.Scan() && loop {
		// Don't fetch header
		if read < 0 && len(header) == 0 {
			// If the first line and there is no header saved yet
			// Read it as header
			header = strings.Split(c.scanner.Text(), string(c.textObjectSetting.Delimeter))

			read++
			continue
		}

		data := c.scanner.Text()
		// Check if the data is match with given filter
		ok, err := isIncluded(strings.Split(data, string(c.textObjectSetting.Delimeter)), header, c.filter)
		if err != nil {
			c.SetError(err)
			return c
		}

		if ok {
			// Mark as read, because it match with the filter
			read++

			// Check if we shoul put it in the fetched data
			// Because of skip function we need to make sure that read is above or equal with 0
			if read >= 0 {
				// If match then convert text to the type of given buffer element
				iv := reflect.New(v).Interface()
				err = textToObj(data, iv, c.textObjectSetting, header...)
				if err != nil {
					err = toolkit.Errorf("unable to serialize data. %s - %s", data, err.Error())
					c.SetError(err)
					return c
				}

				// Append it to fetched data
				ivs = reflect.Append(ivs, reflect.ValueOf(iv).Elem())
			}

			// Check if readed data already statify the required data to be readed
			if read == shouldFetched {
				// If yes stop the loop
				loop = false
			}
		}
	}

	// Set the buffer with fetchedData
	reflect.Indirect(reflect.ValueOf(result)).Set(ivs)

	// Check if have aggregation command
	if hasAggr {
		// Get the aggregation parameter
		items := aggrs.Value.([]*dbflex.AggrItem)

		// Get the group parameter if available
		// If not then pass empty slice
		groups := []string{}
		if hasGroup {
			groups = groupby.Value.([]string)
		}

		// Use aggregate helper
		aggrResults, err := aggregate(result, items, groups...)
		if err != nil {
			c.SetError(err)
			return c
		}

		// Reset the fetched data
		ivs = reflect.MakeSlice(reflect.SliceOf(v), 0, 0)

		// Get all the keys
		aggrKeys := []string{}
		if len(aggrResults) > 0 {
			for k := range aggrResults[0].(toolkit.M) {
				aggrKeys = append(aggrKeys, k)
			}
		}

		// Iterate trhough aggregation result
		for _, ar := range aggrResults {
			aggrResult := ar.(toolkit.M)
			// Create new empty variable with the type of given buffer element
			iv := reflect.New(v).Elem()

			// Iterate through the aggregation keys
			for _, k := range aggrKeys {
				// We need to check wether the buffer element type is kind of struct or map
				// Because golang handle it differently and the function needed to set the value is also different
				// If the buffer element type is kind of struct
				if iv.Kind() == reflect.Struct {
					// Set the field value to the respective field name
					iv.FieldByName(k).Set(reflect.ValueOf(aggrResult[k]))
				} else if iv.Kind() == reflect.Map {
					// If kind of map check first is the map already initialized or not
					if iv.IsNil() {
						// If not then initiatize it
						iv.Set(reflect.MakeMap(v))
					}

					// Set the map value with the respective key name
					iv.SetMapIndex(reflect.ValueOf(k), reflect.ValueOf(aggrResult[k]))
				}
			}

			// Append it to fetched data
			ivs = reflect.Append(ivs, iv)
		}

		// Set the buffer with aggregated result
		reflect.Indirect(reflect.ValueOf(result)).Set(ivs)
	}

	// If there is a sort command
	if hasSort {
		// Then sort it first with given fields
		shouldSortedFields := sortBy.Value.([]string)
		// Start from the last field first to correctly sort the data
		for i := len(shouldSortedFields) - 1; i >= 0; i-- {
			err := byField(reflect.ValueOf(result).Elem().Interface(), shouldSortedFields[i])
			if err != nil {
				c.SetError(err)
				return c
			}
		}
	}

	if (hasSort || hasAggr || hasGroup) && (skip != 0 || take != 0) {
		rv := reflect.Indirect(reflect.ValueOf(result))

		start := skip
		end := take + skip

		if take+skip > rv.Len() {
			end = rv.Len()
		}

		ivs := rv.Slice(start, end)
		rv.Set(ivs)
	}

	return c
}

// Count return count of data with give filter
// BUG: Only filter that applied in this function, group by is not yet implemented
func (c *Cursor) Count() int {
	// Start from -1 because of file header
	if c.scanner == nil {
		c.openFile()
	}

	// Reset file pointer
	c.f.Seek(0, 0)

	header := []string{}
	read := -1

	for c.scanner.Scan() {
		// Don't fetch header
		if read < 0 {
			tempHeader := strings.Split(c.scanner.Text(), string(c.textObjectSetting.Delimeter))
			for _, v := range tempHeader {
				header = append(header, strings.Trim(v, "\""))
			}

			read++
			continue
		}

		data := c.scanner.Text()
		ok, _ := isIncluded(strings.Split(data, string(c.textObjectSetting.Delimeter)), header, c.filter)
		if ok {
			read++
		}
	}

	return read
}

// Close the current file
func (c *Cursor) Close() error {
	e := c.Error()
	if c.f != nil {
		c.f.Close()

		c.f = nil
		c.scanner = nil
	}
	return e
}

func (c *Cursor) openFile() {
	c.SetError(nil)
	f, err := os.Open(c.filePath)
	if err != nil {
		c.SetError(err)
		return
	}

	scanner := bufio.NewScanner(f)
	c.f = f
	c.scanner = scanner
}
