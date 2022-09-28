package json

import (
	"encoding/json"
	"os"
	"reflect"

	"github.com/eaciit/toolkit"

	"git.kanosolution.net/kano/dbflex"
)

// Cursor responsible for converting the text data to given buffer
// For this driver cursor also responsible for filtering, aggregating, grouping, and sorting
type Cursor struct {
	dbflex.CursorBase

	f        *os.File
	filePath string
	filter   *dbflex.Filter
	extra    dbflex.QueryItems
}

var _ dbflex.ICursor = &Cursor{}

// Fetch single data
func (c *Cursor) Fetch(out interface{}) dbflex.ICursor {
	sliceOfT := reflect.SliceOf(reflect.TypeOf(out))
	ptr := reflect.New(sliceOfT)
	ptr.Elem().Set(reflect.MakeSlice(sliceOfT, 0, 0))
	buffer := ptr.Interface()

	err := c.Fetchs(buffer, 1)
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

	read := 0
	shouldFetched := 0

	if !hasSort && !hasAggr && !hasGroup {
		read = -skip
		shouldFetched = take
	}

	v := reflect.TypeOf(result).Elem().Elem()
	// Create empty slice of buffer element type
	ivs := reflect.MakeSlice(reflect.SliceOf(v), 0, 0)

	// Open file
	file, err := os.Open(c.filePath)
	if err != nil {
		c.SetError(err)
		return c
	}
	// Don't forget to close ;)
	defer file.Close()

	// Initiate new decoder from stream
	decoder := json.NewDecoder(file)
	// Read open bracket
	_, err = decoder.Token()
	if err != nil {
		c.SetError(err)
		return c
	}

	// Check if there is more data
	for decoder.More() {
		data := toolkit.M{}
		// Decode data one by one
		err := decoder.Decode(&data)
		if err != nil {
			c.SetError(err)
			return c
		}

		// Check if the data is match with given filter
		ok, err := isIncluded(data, c.filter)
		if err != nil {
			c.SetError(err)
			return c
		}

		if ok {
			// Mark as read, because it match with the filter
			read++

			// Check if we shoul put it in the fetched data
			// Because of skip function we need to make sure that read is above or equal with 0
			if read > 0 {
				// If match then convert text to the type of given buffer element
				iv := reflect.New(v).Interface()
				err = mapToObject(data, iv)
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
				break
			}
		}
	}

	// Read closing bracket
	_, err = decoder.Token()
	if err != nil {
		c.SetError(err)
		return c
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
			err := byField(reflect.Indirect(reflect.ValueOf(result)).Interface(), shouldSortedFields[i])
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
	read := 0

	// Open file
	file, err := os.Open(c.filePath)
	if err != nil {
		return 0
	}
	// Don't forget to close ;)
	defer file.Close()

	// Initiate new decoder from stream
	decoder := json.NewDecoder(file)
	// Read open bracket
	_, err = decoder.Token()
	if err != nil {
		return 0
	}

	// Check if there is more data
	for decoder.More() {
		data := toolkit.M{}

		decoder.Decode(&data)
		ok, _ := isIncluded(data, c.filter)
		if ok {
			read++
		}
	}

	// Read closing bracket
	_, err = decoder.Token()
	if err != nil {
		return 0
	}

	return read
}
