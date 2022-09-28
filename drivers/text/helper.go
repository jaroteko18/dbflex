package text

import (
	"errors"
	"fmt"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"time"

	"git.kanosolution.net/kano/dbflex"

	"github.com/eaciit/toolkit"
)

// WriteMode is a string type that has basically two value which are strict and loose
type WriteMode string

const (
	// ModeStrict means that the current header in the table is absolute.
	// This mean that when inserting new value to the current table that has ModeStrict must have all the fields that the header has.
	// If there is extra field in the data, then it will ignore it and only takes fields that defined in the header.
	ModeStrict WriteMode = "strict"
	// ModeLoose means that the current header in the table is flexisble.
	// This mean that when inserting new value to the current table the data is not must have all the fields that the header has.
	// If there is extra field in the data, then it will add the new field as new header in the end of the line.
	ModeLoose = "loose"
)

// Config is object that holds the settings
type Config struct {
	Delimeter   rune
	UseSign     bool
	Signs       [][]rune
	DateFormats map[string]string
	WriteMode   WriteMode
}

// NewConfig create new TextObjectSetting with default value and given delimiter
func NewConfig(delimeter rune) *Config {
	cfg := new(Config)
	cfg.Delimeter = delimeter
	cfg.UseSign = true
	cfg.Signs = [][]rune{[]rune{'"'}, []rune{'\''}}
	cfg.WriteMode = ModeStrict
	return cfg
}

// SetUseSign set
func (t *Config) SetUseSign(b bool) *Config {
	t.UseSign = b
	return t
}

// SetSign set a signs
func (t *Config) SetSign(runes ...rune) *Config {
	if len(runes) == 1 {
		t.Signs = append(t.Signs, []rune{runes[0]})
	} else if len(runes) > 2 {
		t.Signs = append(t.Signs, []rune{runes[0], runes[1]})
	}
	return t
}

// SetDateFormat set a date format for the give key
func (t *Config) SetDateFormat(key, value string) *Config {
	if t.DateFormats == nil {
		t.DateFormats = map[string]string{}
	}
	t.DateFormats[key] = value
	return t
}

// DateFormat return the toolkit version of date format with given key.
// If specified key is not exist then, it check for specified default date format which is date format with empty string key.
// If that also not exist, then it will return dbflex default date format which is "yyyy-MM-dd HH:mm:ss".
func (t *Config) DateFormat(key string) string {
	if t.DateFormats == nil {
		t.DateFormats = map[string]string{}
	}
	if f, ok := t.DateFormats[key]; ok {
		return f
	}

	if f, ok := t.DateFormats[""]; ok {
		return f
	}

	return "yyyy-MM-dd HH:mm:ss T"
}

func textToObj(txt string, out interface{}, cfg *Config, headers ...string) error {
	vt := reflect.Indirect(reflect.ValueOf(out)).Type()
	if len(headers) == 0 && vt.Kind() == reflect.Struct {
		for i := 0; i < vt.NumField(); i++ {
			name := vt.FieldByIndex([]int{i}).Name
			headers = append(headers, name)
		}
	}

	var closeQuote rune
	var processBufferToObj bool

	inQuote := false
	txtBuff := ""
	idx := 0

	i := 0
	runesTxt := []rune(txt)
	startQuotePos := -1
	bufferQuotePos := -1

	for i < len(runesTxt) {
		char := runesTxt[i]
		addRune := true
		processBufferToObj = false
		if cfg.UseSign {
			if !inQuote {
				for _, sign := range cfg.Signs {
					if len(sign) > 0 {
						if char == sign[0] {
							inQuote = true
							startQuotePos = i
							bufferQuotePos = len(txtBuff)
							if len(sign) == 1 {
								closeQuote = char
							} else {
								closeQuote = sign[1]
							}

							addRune = false
							break
						}
					}
				}
			} else if char == closeQuote {
				inQuote = false
				addRune = false
			}
		}

		if char == cfg.Delimeter && !inQuote {
			addRune = false
			processBufferToObj = true
		}

		if addRune {
			txtBuff += string(char)
		}

		// Close quote is missing
		if i == len(runesTxt)-1 && inQuote && char != closeQuote {
			inQuote = false
			i = startQuotePos
			txtBuff = txtBuff[:bufferQuotePos] + string(closeQuote)
		}

		if processBufferToObj {
			fieldname := ""
			if idx < len(headers) {
				fieldname = headers[idx]
			} else {
				fieldname = toolkit.ToString(idx)
			}
			processTxtToObjField(txtBuff, out, fieldname, cfg)
			txtBuff = ""
			idx++
		}

		i++
	}

	//-- process buff if last operation doesnt does it
	if !processBufferToObj {
		fieldname := ""
		if idx < len(headers) {
			fieldname = headers[idx]
		} else {
			fieldname = toolkit.ToString(idx)
		}
		processTxtToObjField(txtBuff, out, fieldname, cfg)
	}

	return nil
}

func processTxtToObjField(txt string, obj interface{}, fieldname string, cfg *Config) error {
	rv := reflect.Indirect(reflect.ValueOf(obj))
	rt := rv.Type()

	if rt.Kind() == reflect.Ptr {
		if rv.IsNil() == true {
			rv.Set(reflect.New(rt.Elem()))
		}

		rv = rv.Elem()
		rt = rv.Type()
	}

	var objField interface{}

	if rt.Kind() == reflect.Map {
		keyType := rt.Key()
		if keyType.Kind() == reflect.String {
			//--- first convert to time.Time
			dateFormat := cfg.DateFormat(fieldname)
			objField = toolkit.String2Date(txt, dateFormat)
			txtStr := toolkit.Date2String(objField.(time.Time), dateFormat)

			//--- if fails then do number float
			if txtStr != txt {
				number := toolkit.ToFloat64(txt, 4, toolkit.RoundingAuto)
				if number != float64(0) {
					objField = number
				} else if number == 0 && txt == "0" {
					objField = float64(0)
				} else {
					//--- if fails then do string
					objField = txt
				}
			}

			if rv.IsNil() {
				rv.Set(reflect.MakeMap(rt))
			}

			subFieldNames := strings.Split(fieldname, ".")

			for i, subFieldName := range subFieldNames {
				if i < len(subFieldNames)-1 {
					index := reflect.ValueOf(subFieldName)
					newRv := rv.MapIndex(index)

					if newRv.IsValid() {
						rv = reflect.ValueOf(newRv.Interface())
					} else {
						rv.SetMapIndex(index, reflect.MakeMap(rt))
						rv = reflect.ValueOf(rv.MapIndex(index).Interface())
					}
				} else {
					rv.SetMapIndex(reflect.ValueOf(subFieldName), reflect.ValueOf(objField))
				}
			}

		} else {
			return errors.New("output type is a map and need to have string as its key")
		}
	} else {
		subFieldNames := strings.Split(fieldname, ".")

		numfield := rt.NumField()
		for i, subFieldName := range subFieldNames {
			for fieldIdx := 0; fieldIdx < numfield; fieldIdx++ {
				rft := rt.FieldByIndex([]int{fieldIdx})
				name := rft.Name
				if tag, ok := rft.Tag.Lookup("sql"); ok {
					name = tag
				}

				if strings.ToLower(name) == strings.ToLower(subFieldName) {
					if i != len(subFieldNames)-1 {
						rv = rv.FieldByIndex([]int{fieldIdx})
						rt = rv.Type()
						numfield = rt.NumField()
						fieldIdx = 0
						break
					} else {
						castResult := func() string {
							defer func() {
								if r := recover(); r != nil {
									//-- do nothing
								}
							}()

							objField = textToInterface(txt, rft.Type, cfg.DateFormat(subFieldName))

							rfv := rv.FieldByIndex([]int{fieldIdx})
							rfv.Set(reflect.ValueOf(objField))

							return "OK"
						}()
						if castResult != "OK" {
							return toolkit.Errorf("unable to cast %s to %s", txt, rft.Type.String())
						}
						break
					}
				}
			}
		}
	}

	return nil
}

func textToInterface(txt string, receiverType reflect.Type, dateFormat string) interface{} {
	typeName := receiverType.Name()
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
		objField = toolkit.ToDate(txt, dateFormat)
	} else {
		objField = txt
	}
	return objField
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
			// Check of there is a sql tag for this field
			tag, ok := f.Tag.Lookup("sql")

			// If the type is struct but not time.Time or is a map
			if (f.Type.Kind() == reflect.Struct && f.Type != reflect.TypeOf(time.Time{})) || f.Type.Kind() == reflect.Map {
				// Then we need to call this function again to fetch the sub value
				var err error
				var subRes toolkit.M

				// If there is a sql tag, use it as parent name instead of the original field name
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
			// If there is a sql tag, use it as name instead of the original field name
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

func objToText(data interface{}, header []string, cfg *Config) (string, error) {
	m, err := objToM(data)
	if err != nil {
		return "", err
	}

	txts := []string{}
	for _, name := range header {

		if _, ok := m[name]; !ok {
			if cfg.WriteMode == ModeStrict {
				return "", toolkit.Errorf("Field %s is not included in data", name)
			}

			txts = append(txts, "")
			continue
		}

		rvfield := reflect.Indirect(reflect.ValueOf(m[name]))
		fieldName := name

		txt := ""
		kind := rvfield.Kind()

		if rvfield.IsValid() {
			if val, ok := rvfield.Interface().(time.Time); ok {
				txt = toolkit.Date2String(val, cfg.DateFormat(fieldName))
			} else if kind == reflect.Struct || kind == reflect.Map || kind == reflect.Interface {
				txt, err = objToText(rvfield.Interface(), header, cfg)
				if err != nil {
					return "", err
				}
			} else if kind == reflect.Int || kind == reflect.Int16 || kind == reflect.Int32 || kind == reflect.Int64 || kind == reflect.Int8 {
				txt = fmt.Sprintf("%d", rvfield.Int())
			} else if kind == reflect.Float32 || kind == reflect.Float64 {
				txt = fmt.Sprintf("%f", rvfield.Float())
			} else if kind == reflect.String {
				txt = fmt.Sprintf("\"%s\"", rvfield.String())
			}
		}

		txts = append(txts, txt)
	}

	return strings.Join(txts, string(cfg.Delimeter)), nil
}

func interfaceToText(data interface{}, fieldName string, cfg *Config) string {
	rvfield := reflect.Indirect(reflect.ValueOf(data))

	txt := ""
	kind := rvfield.Kind()

	if rvfield.IsValid() {
		if val, ok := rvfield.Interface().(time.Time); ok {
			txt = toolkit.Date2String(val, cfg.DateFormat(fieldName))
		} else if kind == reflect.Int || kind == reflect.Int16 || kind == reflect.Int32 || kind == reflect.Int64 || kind == reflect.Int8 {
			txt = fmt.Sprintf("%d", rvfield.Int())
		} else if kind == reflect.Float32 || kind == reflect.Float64 {
			txt = fmt.Sprintf("%f", rvfield.Float())
		} else if kind == reflect.String {
			txt = fmt.Sprintf("\"%s\"", rvfield.String())
		}
	}

	return txt
}

func objHeader(data interface{}) []string {
	m, _ := objToM(data)
	return m.Keys()
}

// isIncluded return true if the data is match with the given filter, if not then return false.
// If there is an error while checking the data, then it return false, and the error
func isIncluded(data []string, header []string, f *dbflex.Filter) (bool, error) {
	// if the filter is nil, we assume that the caller want to show all the data
	// So we return true
	if f == nil {
		return true, nil
	}

	// Find if the filtered field name is exist in the data
	i := -1
	for k, v := range header {
		if strings.ToLower(v) == strings.ToLower(f.Field) {
			i = k
			break
		}
	}

	// If the field is not found and filter operatrion is not AND, OR, RANGE return error
	if i < 0 && f.Op != dbflex.OpAnd && f.Op != dbflex.OpOr && f.Op != dbflex.OpRange && f.Op != dbflex.OpNot {
		return false, toolkit.Errorf("Field with name %s is not exist in the table", f.Field)
	}

	// Get the data value if field name is found
	dataValue := ""
	if i >= 0 {
		dataValue = strings.Trim(data[i], "\"")
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
		firstResult, err := isIncluded(data, header, dbflex.Gte(f.Field, f.Value.([]interface{})[0]))
		if err != nil {
			return false, err
		}

		// If not return false
		if !firstResult {
			return false, nil
		}

		// Check if the data is LTE than second filter value
		secondResult, err := isIncluded(data, header, dbflex.Lte(f.Field, f.Value.([]interface{})[1]))
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
			r, err := isIncluded(data, header, ff)
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
			r, err := isIncluded(data, header, ff)
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
		r, err := isIncluded(data, header, f.Items[0])
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

func verifyHeader(header []string, data interface{}) bool {
	objectHeader := objHeader(data)
	for _, h := range header {
		ok := false
		for _, v := range objectHeader {
			if strings.ToLower(v) == strings.ToLower(h) {
				ok = true
				break
			}
		}

		if !ok {
			return false
		}
	}

	return true
}

func combineHeader(originalHeader, dataHeader []string) []string {
	finalHeader := []string{}
	if len(originalHeader) == 0 {
		finalHeader = dataHeader
	} else {
		for _, h := range originalHeader {
			found := false
			for _, dh := range dataHeader {
				if strings.ToLower(h) == strings.ToLower(dh) {
					finalHeader = append(finalHeader, dh)
					found = true
					break
				}
			}

			if !found {
				finalHeader = append(finalHeader, h)
			}
		}

		for _, dh := range dataHeader {
			found := false
			for _, fh := range finalHeader {
				if dh == fh {
					found = true
					break
				}
			}

			if !found {
				finalHeader = append(finalHeader, dh)
			}
		}
	}

	return finalHeader
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
