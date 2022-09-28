package json

import (
	"encoding/json"
	"fmt"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"git.kanosolution.net/kano/dbflex"
	"github.com/eaciit/toolkit"
)

func writeToJSONFile(data interface{}, file *os.File) error {
	// Truncate file
	file.Truncate(0)
	// Reset cursor pointer
	file.Seek(0, 0)

	// Initiate new encoder
	encoder := json.NewEncoder(file)
	// Encode the data
	err := encoder.Encode(data)
	if err != nil {
		return err
	}
	// Sync file
	file.Sync()

	return nil
}

// Convert object or maps to flat toolkit.M
// This function is completely different with toolkit.ToM
// In here we use reflect to keep its original value and type
// instead of serde using json like toolkit.M do that will convert everything to text (CMIIW)
func objToM(data interface{}, parents ...string) (toolkit.M, error) {
	rv := reflect.Indirect(reflect.ValueOf(data))
	// Create emapty map as a result
	res := toolkit.M{}

	// If passed data have nested object / value then we need to keep track of the parents name
	// Use "." (dot) separator as the level separator
	prefix := ""
	if len(parents) > 0 {
		prefix = strings.Join(parents, ".") + "."
	}

	// Because of the difference behaviour of Struct type and Map type, we need to check the data element type
	if rv.Kind() == reflect.Struct {
		// Iterate through all the available field
		for i := 0; i < rv.NumField(); i++ {
			// Get the field type
			f := rv.Type().Field(i)
			// Check of there is a json tag for this field
			tag, ok := f.Tag.Lookup("json")

			// If the type is struct but not time.Time or is a map
			if (f.Type.Kind() == reflect.Struct && f.Type != reflect.TypeOf(time.Time{})) || f.Type.Kind() == reflect.Map {
				// Then we need to call this function again to fetch the sub value
				var err error
				var subRes toolkit.M

				// If there is a json tag, use it as parent name instead of the original field name
				if ok {
					subRes, err = objToM(rv.Field(i).Interface(), append(parents, tag)...)
				} else {
					subRes, err = objToM(rv.Field(i).Interface(), append(parents, f.Name)...)
				}

				if err != nil {
					return nil, err
				}

				// For all the sub value put it in the result
				for k, v := range subRes {
					res[k] = v
				}

				// Skip the rest
				continue
			}

			// If the type is time.Time or is not struct and map then put it in the result directly
			// If there is a json tag, use it as name instead of the original field name
			if ok {
				res[prefix+tag] = rv.Field(i).Interface()
			} else {
				res[prefix+f.Name] = rv.Field(i).Interface()
			}
		}

		// Return the result
		return res, nil
	} else if rv.Kind() == reflect.Map {
		// If the data element is kind of map
		// Iterate through all avilable keys
		for _, key := range rv.MapKeys() {
			// Get the map value type of the specified key
			t := rv.MapIndex(key).Elem().Type()
			// If the type is struct but not time.Time or is a map
			if (t.Kind() == reflect.Struct && t != reflect.TypeOf(time.Time{})) || t.Kind() == reflect.Map {
				// Then we need to call this function again to fetch the sub value
				subRes, err := objToM(rv.MapIndex(key).Interface(), append(parents, key.String())...)
				if err != nil {
					return nil, err
				}

				// For all the sub value put it in the result
				for k, v := range subRes {
					res[k] = v
				}

				// Skip the rest
				continue
			}

			// If the type is time.Time or is not struct and map then put it in the result directly
			res[prefix+key.String()] = rv.MapIndex(key).Interface()
		}

		// Return the result
		return res, nil
	}

	// If the data element is not map or struct then return error
	return nil, toolkit.Errorf("Expecting struct or map object but got %s", rv.Kind())
}

func textToInterface(txt string, receiverType reflect.Type) interface{} {
	typeName := receiverType.String()
	var objField interface{}
	if typeName == "string" {
		objField = txt
	} else if strings.HasPrefix(typeName, "int") && receiverType.Kind() != reflect.Interface {
		objField = toolkit.ToInt(txt, toolkit.RoundingAuto)
	} else if typeName == "float32" {
		objField = toolkit.ToFloat32(txt, 4, toolkit.RoundingAuto)
	} else if typeName == "float64" {
		objField = toolkit.ToFloat64(txt, 4, toolkit.RoundingAuto)
	} else if receiverType == reflect.TypeOf(time.Time{}) {
		objField, _ = time.Parse(time.RFC3339, txt)
	} else {
		objField = txt
	}
	return objField
}

func mapToObject(data map[string]interface{}, result interface{}, parents ...string) error {
	rv := reflect.Indirect(reflect.ValueOf(result))
	t := rv.Type()

	if t.Kind() == reflect.Ptr {
		if rv.IsNil() == true {
			rv.Set(reflect.New(t.Elem()))
		}

		rv = rv.Elem()
		t = rv.Type()
	}

	for _, p := range parents {
		rv = rv.FieldByName(p)
	}

	if t.Kind() == reflect.Map {
		if rv.IsNil() {
			rv.Set(reflect.MakeMap(t))
		}

		for k, v := range data {
			rv.SetMapIndex(reflect.ValueOf(k), reflect.ValueOf(v))
		}
	} else if t.Kind() == reflect.Struct {
		for k, v := range data {
			sv := rv.FieldByName(k)

			if !sv.IsValid() {
				for i := 0; i < rv.NumField(); i++ {
					f := rv.Type().Field(i)
					tag, ok := f.Tag.Lookup("json")

					if ok && tag == k {
						sv = rv.Field(i)
						break
					}
				}
			}

			if sv.IsValid() {
				kind := sv.Type().Kind()
				if sv.Type() != reflect.TypeOf(time.Time{}) && (kind == reflect.Struct || kind == reflect.Map) {
					mapToObject(v.(map[string]interface{}), result, k)

					continue
				}

				cv := textToInterface(fmt.Sprint(v), sv.Type())
				sv.Set(reflect.ValueOf(cv))
			}
		}
	} else {
		return toolkit.Errorf("Unexpected type of %s", t.Kind())
	}

	return nil
}

// isIncluded return true if the data is match with the given filter, if not then return false.
// If there is an error while checking the data, then it return false, and the error
func isIncluded(data toolkit.M, f *dbflex.Filter) (bool, error) {
	// if the filter is nil, we assume that the caller want to show all the data
	// So we return true
	if f == nil {
		return true, nil
	}

	// Find if the filtered field name is exist in the data
	subNames := strings.Split(f.Field, ".")
	keys := []string{}
	for _, s := range subNames {
		lSubName := strings.ToLower(s)
		for _, v := range data.Keys() {
			if strings.ToLower(v) == lSubName {
				keys = append(keys, v)

				if len(keys) != len(subNames) {
					data = data[s].(map[string]interface{})
				}

				break
			}
		}
	}

	// If the field is not found and filter operatrion is not AND, OR, RANGE return error
	if len(keys) != len(subNames) && f.Op != dbflex.OpAnd && f.Op != dbflex.OpOr && f.Op != dbflex.OpRange && f.Op != dbflex.OpNot {
		return false, toolkit.Errorf("Field with name %s is not exist in the table", f.Field)
	}

	// Get the data value if field name is found
	dataValue := ""
	if len(keys) > 0 {
		dataValue = fmt.Sprint(data[keys[len(keys)-1]])
	}

	// Check the field operation and do operation accordingly
	if f.Op == dbflex.OpEq {
		if dataValue != fmt.Sprint(f.Value) {
			return false, nil
		}
	} else if f.Op == dbflex.OpNe {
		if dataValue == fmt.Sprint(f.Value) {
			return false, nil
		}
	} else if f.Op == dbflex.OpContains {
		keywords := f.Value.([]string)
		match := false
		for _, keyword := range keywords {
			if strings.Contains(strings.ToLower(dataValue), strings.ToLower(keyword)) {
				match = true
				break
			}
		}

		return match, nil
	} else if f.Op == dbflex.OpStartWith {
		return strings.HasPrefix(dataValue, fmt.Sprint(f.Value)), nil
	} else if f.Op == dbflex.OpEndWith {
		return strings.HasSuffix(dataValue, fmt.Sprint(f.Value)), nil
	} else if f.Op == dbflex.OpIn {
		keywords := f.Value.([]string)
		match := false
		for _, keyword := range keywords {
			if strings.ToLower(dataValue) == strings.ToLower(keyword) {
				match = true
				break
			}
		}

		return match, nil
	} else if f.Op == dbflex.OpNin {
		keywords := f.Value.([]string)
		match := true
		for _, keyword := range keywords {
			if strings.ToLower(dataValue) == strings.ToLower(keyword) {
				match = false
				break
			}
		}

		return match, nil
	} else if f.Op == dbflex.OpGt {
		v, err := strconv.ParseFloat(fmt.Sprint(dataValue), 64)
		if err != nil {
			return false, err
		}

		c, err := strconv.ParseFloat(fmt.Sprint(f.Value), 64)
		if err != nil {
			return false, err
		}

		return v > c, nil
	} else if f.Op == dbflex.OpGte {
		v, err := strconv.ParseFloat(fmt.Sprint(dataValue), 64)
		if err != nil {
			return false, err
		}

		c, err := strconv.ParseFloat(fmt.Sprint(f.Value), 64)
		if err != nil {
			return false, err
		}

		return v >= c, nil
	} else if f.Op == dbflex.OpLt {
		v, err := strconv.ParseFloat(fmt.Sprint(dataValue), 64)
		if err != nil {
			return false, err
		}

		c, err := strconv.ParseFloat(fmt.Sprint(f.Value), 64)
		if err != nil {
			return false, err
		}

		return v < c, nil
	} else if f.Op == dbflex.OpLte {
		v, err := strconv.ParseFloat(fmt.Sprint(dataValue), 64)
		if err != nil {
			return false, err
		}

		c, err := strconv.ParseFloat(fmt.Sprint(f.Value), 64)
		if err != nil {
			return false, err
		}

		return v <= c, nil
	} else if f.Op == dbflex.OpRange {
		// If filter operation is RANGE that means value should be slice with first value is the lowest and second value is the highest
		// Check if the data is GTE than first filter value
		firstResult, err := isIncluded(data, dbflex.Gte(f.Field, f.Value.([]interface{})[0]))
		if err != nil {
			return false, err
		}

		// If not return false
		if !firstResult {
			return false, nil
		}

		// Check if the data is LTE than second filter value
		secondResult, err := isIncluded(data, dbflex.Lte(f.Field, f.Value.([]interface{})[1]))
		if err != nil {
			return false, err
		}

		// Both first and second should return true so we use AND operand
		return firstResult && secondResult, nil
	} else if f.Op == dbflex.OpOr {
		// If filter operation is OR then
		fs := f.Items
		// Create past result variable as false
		match := false
		// Iterate through all filter items
		for _, ff := range fs {
			// Get the result for each filter item
			r, err := isIncluded(data, ff)
			if err != nil {
				return false, err
			}

			// Combine past result with current result using OR operand
			match = match || r
			// If match is true return true
			// (that means one filter item is true so we ignore the rest of results)
			if match {
				return true, nil
			}
		}

		return match, nil
	} else if f.Op == dbflex.OpAnd {
		// If filter operation is AND then
		fs := f.Items
		// Create past result variable as true
		match := true
		// Iterate through all filter items
		for _, ff := range fs {
			// Get the result for each filter item
			r, err := isIncluded(data, ff)
			if err != nil {
				return false, err
			}

			// Combine past result with current result using AND operand
			match = match && r
			// If match is false return false
			// (that means one of the filter items result is false so we ignore the rest of results)
			if !match {
				return false, nil
			}
		}

		return match, nil
	} else if f.Op == dbflex.OpNot {
		r, err := isIncluded(data, f.Items[0])
		if err != nil {
			return false, err
		}

		// Not operation here
		return !r, nil
	} else {
		// If the operand is not one of the above then return error
		return false, fmt.Errorf("Filter Op %s is not defined", f.Op)
	}

	return true, nil
}

// AggregatorHelper
func aggregate(data interface{}, aggrItems []*dbflex.AggrItem, groups ...string) ([]interface{}, error) {
	rv := reflect.ValueOf(reflect.ValueOf(data).Elem().Interface())
	// If len of the data is 0 then return empty slice of interface as result
	if rv.Len() == 0 {
		return []interface{}{}, nil
	}

	opResults := toolkit.M{}
	aggregatedFieldNames := map[string]string{}
	groupedFieldNames := []string{}
	v1 := rv.Index(0)

	// Before fetching all the aggregated fieldNames and groupedFieldNames
	// We need to check the type of the data element because of different behaviour of map and struct type
	if v1.Kind() == reflect.Struct {
		for i := 0; i < v1.NumField(); i++ {
			name := v1.Type().Field(i).Name
			lname := strings.ToLower(name)
			for _, item := range aggrItems {
				if lname == strings.ToLower(item.Field) {
					aggregatedFieldNames[item.Field] = name
				}
			}

			for _, group := range groups {
				if lname == strings.ToLower(group) {
					groupedFieldNames = append(groupedFieldNames, name)
				}
			}
		}
	} else if v1.Kind() == reflect.Map {
		for _, mk := range v1.MapKeys() {
			name := mk.String()
			lname := strings.ToLower(name)
			for _, item := range aggrItems {
				if lname == strings.ToLower(item.Field) {
					aggregatedFieldNames[item.Field] = name
				}
			}

			for _, group := range groups {
				if lname == strings.ToLower(group) {
					groupedFieldNames = append(groupedFieldNames, name)
				}
			}
		}
	} else {
		return nil, toolkit.Errorf("Expecting struct or map object got %s", v1.String())
	}

	// Iterate through all aggregation items
	for _, item := range aggrItems {
		name := aggregatedFieldNames[item.Field]
		// Assume type of the aggregated field as Float64
		kind := reflect.Float64

		// If the data element type is Struct
		if v1.Kind() == reflect.Struct {
			// Then get the type of aggregated field
			kind = v1.FieldByName(name).Kind()
		}

		switch kind {
		case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:

			// Iterate through all the data
			for j := 0; j < rv.Len(); j++ {
				vj := rv.Index(j)
				// Create empty keyID as our unique key
				keyID := ""
				// For all the grouped field names, get the value and add it to the keyID
				// Example: the are 3 field in the grouped field names
				// So the keyID will be like this "VuleField1-ValueField2-ValueField3"
				for _, rg := range groupedFieldNames {
					keyID += "-" + fmt.Sprint(getValueOf(vj, rg))
				}

				var prevResult toolkit.M
				// get the value of aggregated fields as int
				// BUG: if the type are Int8, Int16, Int32, Int64 this will fail
				v := getValueOf(vj, name).(int)

				// Check if the previous results with the specific keyID is already exist
				r, exist := opResults[keyID]
				if !exist {
					// If not exist then create empty M
					prevResult = toolkit.M{}
					// And set it as the prevuious result of the specific keyID
					opResults[keyID] = prevResult

					// Set all the value for grouped field
					for _, rg := range groupedFieldNames {
						prevResult[rg] = getValueOf(vj, rg)
					}

					if item.Op == dbflex.AggrMax || item.Op == dbflex.AggrMin {
						// If the aggregation operation is MAX or MIN
						// Set the current value as previous result value
						prevResult[name] = v
					} else if item.Op == dbflex.AggrAvg {
						// If the aggregation operation is AVG
						// Set previous result value to 0
						prevResult[name] = int(0)
						// And set previous count value to 0
						// This count later used as the divider
						prevResult[name+"_count"] = int(0)
					} else {
						// Else set previous result value to 0
						prevResult[name] = int(0)
					}
				} else {
					// If previous result is exist the cast it as toolkit.M
					prevResult = r.(toolkit.M)
				}

				switch item.Op {
				case dbflex.AggrSum:
					prevResult[name] = prevResult[name].(int) + v
				case dbflex.AggrAvg:
					prevResult[name] = prevResult[name].(int) + v
					prevResult[name+"_count"] = prevResult[name+"_count"].(int) + 1
				case dbflex.AggrCount:
					prevResult[name] = prevResult[name].(int) + 1
				case dbflex.AggrMax:
					if v > prevResult[name].(int) {
						prevResult[name] = v
					}
				case dbflex.AggrMin:
					if v < prevResult[name].(int) {
						prevResult[name] = v
					}
				default:
					return nil, toolkit.Error("Unknown aggregation operation")
				}
			}

			// In the end, if the aggregation operation is AVG then
			// Divide each result with its count
			if item.Op == dbflex.AggrAvg {
				for _, r := range opResults {
					v := r.(toolkit.M)
					v[name] = v[name].(int) / v[name+"_count"].(int)
				}
			}

		case reflect.Float32, reflect.Float64:

			// Iterate through all the data
			for j := 0; j < rv.Len(); j++ {
				vj := rv.Index(j)
				// Create empty keyID as our unique key
				keyID := ""
				// For all the grouped field names, get the value and add it to the keyID
				// Example: the are 3 field in the grouped field names
				// So the keyID will be like this "VuleField1-ValueField2-ValueField3"
				for _, rg := range groupedFieldNames {
					keyID += fmt.Sprint(getValueOf(vj, rg))
				}

				var prevResult toolkit.M
				// get the value of aggregated fields as float64
				// BUG: if the type is Float32 this will fail
				v := getValueOf(vj, name).(float64)

				// Check if the previous results with the specific keyID is already exist
				r, exist := opResults[keyID]
				if !exist {
					// If not exist then create empty M
					prevResult = toolkit.M{}
					// And set it as the prevuious result of the specific keyID
					opResults[keyID] = prevResult

					// Set all the value for grouped field
					for _, rg := range groupedFieldNames {
						prevResult[rg] = getValueOf(vj, rg)
					}

					if item.Op == dbflex.AggrMax || item.Op == dbflex.AggrMin {
						// If the aggregation operation is MAX or MIN
						// Set the current value as previous result value
						prevResult[name] = v
					} else if item.Op == dbflex.AggrAvg {
						// If the aggregation operation is AVG
						// Set previous result value to 0
						prevResult[name] = float64(0)
						// And set previous count value to 0
						// This count later used as the divider
						prevResult[name+"_count"] = float64(0)
					} else {
						// Else set previous result value to 0
						prevResult[name] = float64(0)
					}
				} else {
					// If previous result is exist the cast it as toolkit.M
					prevResult = r.(toolkit.M)
				}

				switch item.Op {
				case dbflex.AggrSum, dbflex.AggrAvg:
					prevResult[name] = prevResult[name].(float64) + v
				case dbflex.AggrCount:
					prevResult[name] = prevResult[name].(float64) + 1
				case dbflex.AggrMax:
					if v > prevResult[name].(float64) {
						prevResult[name] = v
					}
				case dbflex.AggrMin:
					if v < prevResult[name].(float64) {
						prevResult[name] = v
					}
				default:
					return nil, toolkit.Error("Unknown aggregation operation")
				}
			}

			// In the end, if the aggregation operation is AVG then
			// Divide each result with its count
			if item.Op == dbflex.AggrAvg {
				for _, r := range opResults {
					v := r.(toolkit.M)
					v[name] = v[name].(float64) / v[name+"_count"].(float64)
				}
			}

			// If the type is not a number return error
		default:
			return nil, toolkit.Errorf("Cannot aggregate %s values", kind.String())
		}
	}

	// Return all the values
	return opResults.Values(), nil
}

func getValueOf(from reflect.Value, name string) interface{} {
	if from.Type().Kind() == reflect.Map {
		return from.MapIndex(reflect.ValueOf(name)).Interface()
	} else if from.Type().Kind() == reflect.Struct {
		return from.FieldByName(name).Interface()
	}

	return nil
}

// byField is a sorting helper extended version of https://play.golang.org/p/YLherBg9_B
func byField(data interface{}, field string) error {
	v := reflect.ValueOf(data)
	if v.Kind() != reflect.Slice {
		return toolkit.Error("byField takes a slice as data ")
	}
	if v.Len() == 0 {
		return nil
	}

	v1 := reflect.Indirect(v.Index(0))
	t := v1.Type()

	reverseOrder := strings.HasPrefix(field, "-")
	fieldName := strings.ToLower(field)
	fieldName = strings.TrimPrefix(fieldName, "-")
	found := false

	var f1 reflect.Value

	if t.Kind() == reflect.Struct {
		for i := 0; i < t.NumField(); i++ {
			name := t.Field(i).Name
			if strings.ToLower(name) == fieldName {
				fieldName = name
				f1 = v1.FieldByName(name)
				found = true
				break
			}
		}
	} else if t.Kind() == reflect.Map {
		for _, key := range v1.MapKeys() {
			name := key.String()
			if strings.ToLower(name) == fieldName {
				fieldName = name
				f1 = v1.MapIndex(key)
				found = true
				break
			}
		}
	} else {
		return toolkit.Error("byField takes a slice of structs or slice of map as data")
	}

	if !found {
		return toolkit.Error("byField cannot find field " + field)
	}

	if f1.Type().Kind() == reflect.Interface {
		f1 = reflect.ValueOf(f1.Interface())
	}

	var helper sort.Interface
	switch f1.Type().Kind() {
	case reflect.Int, reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64:
		helper = byFieldHelperInt{
			v:         v,
			size:      v.Len(),
			tmp:       reflect.New(v.Type().Elem()),
			keyValues: makeIntKeys(v, fieldName, v.Len()),
		}
	case reflect.Float32, reflect.Float64:
		helper = byFieldHelperFloat{
			v:         v,
			size:      v.Len(),
			tmp:       reflect.New(v.Type().Elem()),
			keyValues: makeFloatKeys(v, fieldName, v.Len()),
		}
	case reflect.String:
		helper = byFieldHelperString{
			v:         v,
			size:      v.Len(),
			tmp:       reflect.New(v.Type().Elem()),
			keyValues: makeStringKeys(v, fieldName, v.Len()),
		}
	case reflect.Struct:
		if f1.Type() == reflect.TypeOf(time.Time{}) {
			helper = byFieldHelperTime{
				v:         v,
				size:      v.Len(),
				tmp:       reflect.New(v.Type().Elem()),
				keyValues: makeTimeKeys(v, fieldName, v.Len()),
			}
		} else {
			return toolkit.Error("Cannot compare " + f1.Type().String() + " values")
		}
	default:
		return toolkit.Error("Cannot compare " + f1.Type().String() + " values")
	}

	if reverseOrder {
		sort.Sort(sort.Reverse(helper))
	} else {
		sort.Sort(helper)
	}

	return nil
}

func swapValues(slice reflect.Value, tmp reflect.Value, i, j int) {
	vi := slice.Index(i)
	vj := slice.Index(j)
	tmp.Elem().Set(vi)
	vi.Set(vj)
	vj.Set(tmp.Elem())
}

type byFieldHelperInt struct {
	v         reflect.Value
	size      int
	tmp       reflect.Value
	keyValues []int64
}

func (t byFieldHelperInt) Len() int { return t.size }

func (t byFieldHelperInt) Swap(i, j int) {
	swapValues(t.v, t.tmp, i, j)
	t.keyValues[i], t.keyValues[j] = t.keyValues[j], t.keyValues[i]
}

func (t byFieldHelperInt) Less(i, j int) bool {
	return t.keyValues[i] < t.keyValues[j]
}

func makeIntKeys(v reflect.Value, fieldName string, len int) []int64 {
	keys := make([]int64, len)
	for i := 0; i < len; i++ {
		vi := reflect.Indirect(v.Index(i))
		var fi reflect.Value

		if vi.Type().Kind() == reflect.Struct {
			fi = vi.FieldByName(fieldName)
		} else {
			fi = vi.MapIndex(reflect.ValueOf(fieldName))
			fi = reflect.ValueOf(fi.Interface())
		}

		keys[i] = fi.Int()
	}
	return keys
}

type byFieldHelperString struct {
	v         reflect.Value
	size      int
	tmp       reflect.Value
	keyValues []string
}

func (t byFieldHelperString) Len() int { return t.size }

func (t byFieldHelperString) Swap(i, j int) {
	swapValues(t.v, t.tmp, i, j)
	t.keyValues[i], t.keyValues[j] = t.keyValues[j], t.keyValues[i]
}

func (t byFieldHelperString) Less(i, j int) bool {
	return t.keyValues[i] < t.keyValues[j]
}

func makeStringKeys(v reflect.Value, fieldName string, len int) []string {
	keys := make([]string, len)
	for i := 0; i < len; i++ {
		vi := v.Index(i)
		var fi reflect.Value

		if vi.Type().Kind() == reflect.Struct {
			fi = vi.FieldByName(fieldName)
		} else {
			fi = vi.MapIndex(reflect.ValueOf(fieldName))
			fi = reflect.ValueOf(fi.Interface())
		}

		keys[i] = fi.String()
	}
	return keys
}

type byFieldHelperFloat struct {
	v         reflect.Value
	size      int
	tmp       reflect.Value
	keyValues []float64
}

func (t byFieldHelperFloat) Len() int { return t.size }

func (t byFieldHelperFloat) Swap(i, j int) {
	swapValues(t.v, t.tmp, i, j)
	t.keyValues[i], t.keyValues[j] = t.keyValues[j], t.keyValues[i]
}

func (t byFieldHelperFloat) Less(i, j int) bool {
	return t.keyValues[i] < t.keyValues[j]
}

func makeFloatKeys(v reflect.Value, fieldName string, len int) []float64 {
	keys := make([]float64, len)
	for i := 0; i < len; i++ {
		vi := reflect.Indirect(v.Index(i))
		var fi reflect.Value

		if vi.Type().Kind() == reflect.Struct {
			fi = vi.FieldByName(fieldName)
		} else {
			fi = vi.MapIndex(reflect.ValueOf(fieldName))
			fi = reflect.ValueOf(fi.Interface())
		}

		keys[i] = fi.Float()
	}
	return keys
}

type byFieldHelperTime struct {
	v         reflect.Value
	size      int
	tmp       reflect.Value
	keyValues []time.Time
}

func (t byFieldHelperTime) Len() int { return t.size }

func (t byFieldHelperTime) Swap(i, j int) {
	swapValues(t.v, t.tmp, i, j)
	t.keyValues[i], t.keyValues[j] = t.keyValues[j], t.keyValues[i]
}

func (t byFieldHelperTime) Less(i, j int) bool {
	return t.keyValues[i].Before(t.keyValues[j])
}

func makeTimeKeys(v reflect.Value, fieldName string, len int) []time.Time {
	keys := make([]time.Time, len)
	for i := 0; i < len; i++ {
		vi := reflect.Indirect(v.Index(i))
		var fi reflect.Value

		if vi.Type().Kind() == reflect.Struct {
			fi = vi.FieldByName(fieldName)
		} else {
			fi = vi.MapIndex(reflect.ValueOf(fieldName))
		}

		keys[i] = fi.Interface().(time.Time)
	}
	return keys
}
