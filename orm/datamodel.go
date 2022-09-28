package orm

import (
	"fmt"
	"reflect"
	"strings"

	"git.kanosolution.net/kano/dbflex"
	"github.com/eaciit/toolkit"
)

// DataModel is an interface that should be implemented to a model
type DataModel interface {
	TableName() string
	GetID(dbflex.IConnection) ([]string, []interface{})
	SetID(...interface{})
	SetObjectID(...interface{}) DataModel
	FK() []*FKConfig
	ReverseFK() []*ReverseFKConfig

	GetFilterID(dbflex.IConnection, ...interface{}) *dbflex.Filter
	GetWhereFilter(toolkit.M) *dbflex.Filter
	PreSave(dbflex.IConnection) error
	PostSave(dbflex.IConnection) error
	PreDelete(dbflex.IConnection) error
	PostDelete(dbflex.IConnection) error

	SetThis(DataModel)
	This() DataModel
}

type MetaField struct {
	Name   string
	DbName string
	Type   reflect.Type
}

// DataModelBase is a base struct for easier implementation of DataModel interface
type DataModelBase struct {
	self DataModel

	hasBeenCalc bool
	meta        map[string]MetaField
}

// FK return FKConfig for data model
func (dm *DataModelBase) FK() []*FKConfig {
	return []*FKConfig{}
}

// ReverseFK return FKConfig for data model
func (dm *DataModelBase) ReverseFK() []*ReverseFKConfig {
	return []*ReverseFKConfig{}
}

// NewDataModel abstraction to create new data model object
func NewDataModel(m DataModel) DataModel {
	m.SetThis(m)
	return m
}

func getFieldInfo(d *DataModelBase, fieldNameTag string) {
	if !d.hasBeenCalc {
		v := reflect.ValueOf(d.This())
		if v.Kind() == reflect.Ptr {
			v = v.Elem()
		}

		t := v.Type()
		fieldNum := t.NumField()
		fields := make(map[string]MetaField, fieldNum)
		for i := 0; i < fieldNum; i++ {
			f := t.Field(i)
			tag := f.Tag
			dbname := strings.ToLower(f.Name)
			nameTag := tag.Get(fieldNameTag)
			if nameTag != "" {
				dbname = nameTag
			}
			fields[f.Name] = MetaField{
				Name:   f.Name,
				DbName: dbname,
				Type:   f.Type,
			}
		}
		d.meta = fields
		d.hasBeenCalc = true
	}
}

func getFieldDbName(d *DataModelBase, name, fieldNameTag string) string {
	getFieldInfo(d, fieldNameTag)
	mf, ok := d.meta[name]
	if !ok {
		return ""
	}
	return mf.DbName
}

func getFieldType(d *DataModelBase, name, fieldNameTag string) reflect.Type {
	getFieldInfo(d, fieldNameTag)
	mf, ok := d.meta[name]
	if !ok {
		return reflect.TypeOf("")
	}
	return mf.Type
}

// SetThis to create circle loop to refer to datamodel
func (d *DataModelBase) SetThis(m DataModel) {
	d.self = m
}

// This to get abstraction of datamodel object
func (d *DataModelBase) This() DataModel {
	if d.self == nil {
		return d
	}
	return d.self

}

// Get filter ID
func (d *DataModelBase) TableName() string {
	naturalName := strings.ToLower(reflect.TypeOf(d.This()).Elem().Name())
	switch naturalName[len(naturalName)-1] {
	case 'y':
		return naturalName[0:len(naturalName)-1] + "ies"

	case 's':
		return naturalName + "es"
	}
	return naturalName + "s"
}

// Get filter ID
func (d *DataModelBase) GetFilterID(conn dbflex.IConnection, ids ...interface{}) *dbflex.Filter {
	//panic("GetFilterID not implemented")
	fields, keys := d.This().GetID(conn)
	filters := []*dbflex.Filter{}
	for idx, field := range fields {
		if idx < len(keys) {
			filters = append(filters, dbflex.Eq(field, keys[idx]))
		}
	}
	return dbflex.And(filters...)
}

// Get where filter from any field that not blank
func (d *DataModelBase) GetWhereFilter(toolkit.M) *dbflex.Filter {
	panic("GetWhereFilter not implemented")
}

// GetID to get ID
func (d *DataModelBase) GetID(conn dbflex.IConnection) ([]string, []interface{}) {
	fieldtag := ""
	keynametag := ""
	if conn != nil {
		fieldtag = conn.FieldNameTag()
		keynametag = conn.KeyNameTag()
	}

	if keynametag == "" {
		keynametag = "key"
	}

	v := reflect.ValueOf(d.This()).Elem()
	t := v.Type()

	ids := []string{}
	values := []interface{}{}
	fieldNum := t.NumField()
	for idx := 0; idx < fieldNum; idx++ {
		tf := t.Field(idx)
		fn := strings.Split(tf.Tag.Get(fieldtag), ",")[0]
		if fn == "" {
			fn = tf.Name
		}
		iskey := tf.Tag.Get(keynametag)

		//fmt.Println(t.Name, "|", tf.Name, "|", fn, "|", iskey)
		if iskey != "" {
			ids = append(ids, fn)
			values = append(values, v.Field(idx).Interface())
		}
	}

	if len(ids) == 0 {
		panic("GetID can't be applied for " + t.Name() + ", please check your object definition.")
	}

	return ids, values
}

// SetID is not used yet
func (d *DataModelBase) SetID(keys ...interface{}) {
	//-- do nothing
	panic("SetID is not yet implemented for this DataModel")
}

// SetObjectID is not used yet
func (d *DataModelBase) SetObjectID(keys ...interface{}) DataModel {
	d.This().SetID(keys...)
	return d.This()
}

// PostSave run after data is saved (insert, update, save)
func (d *DataModelBase) PostSave(conn dbflex.IConnection) error {
	//-- do nothing
	return nil
}

// PreSave run before data being saved (insert, update, save)
func (d *DataModelBase) PreSave(conn dbflex.IConnection) error {
	//-- do nothing
	return nil
}

// PreDelete run before data being deleted
func (d *DataModelBase) PreDelete(conn dbflex.IConnection) error {
	//-- do nothing
	return nil
}

// PostDelete run after data is deleted
func (d *DataModelBase) PostDelete(conn dbflex.IConnection) error {
	//-- do nothing
	return nil
}

// Get a single data from given connection and model
// For single data you can make filter directly inside the model itself,
// the downside of this feature is currently you cannot use dbflex.And, Or, Range on the same field
func Get(conn dbflex.IConnection, model DataModel) error {
	model.SetThis(model)
	tablename := model.TableName()
	where := generateFilterFromDataModel(conn, model)
	cursor := conn.Cursor(dbflex.From(tablename).Select().Where(where), toolkit.M{})
	defer cursor.Close()
	e := cursor.Fetch(model).Error()
	//fmt.Println(tablename, "where:", toolkit.JsonString(where), "model:", toolkit.JsonString(model))
	return e
	//return fmt.Errorf("asdada")
}

// GetWhere get a single datamodel
func GetWhere(conn dbflex.IConnection, model DataModel, where *dbflex.Filter) error {
	model.SetThis(model)
	tablename := model.TableName()
	cmd := dbflex.From(tablename).Select().Take(1)
	if where != nil {
		cmd.Where(where)
	}
	return conn.Cursor(cmd, nil).Fetch(model).Close()
}

// Gets multiple data from given connection, model, buffer, and query param
func Gets(conn dbflex.IConnection, model DataModel, buffer interface{}, qp *dbflex.QueryParam) error {
	model.SetThis(model)
	tablename := model.TableName()

	if qp == nil {
		qp = dbflex.NewQueryParam()
	}

	cmd := dbflex.From(tablename)
	if len(qp.Select) == 0 {
		cmd = cmd.Select()
	} else {
		cmd = cmd.Select(qp.Select...)
	}
	if qp != nil {
		if qp.Where != nil {
			cmd.Where(qp.Where)
		}

		if len(qp.Sort) > 0 {
			cmd.OrderBy(qp.Sort...)
		}

		if qp.Skip > 0 {
			cmd.Skip(qp.Skip)
		}

		if qp.Take > 0 {
			cmd.Take(qp.Take)
		}
	}

	cursor := conn.Cursor(cmd, nil)
	defer cursor.Close()
	return cursor.Fetchs(buffer, 0).Error()
}

// Insert new data from given connection and data model
func Insert(conn dbflex.IConnection, dm DataModel) error {
	dm.SetThis(dm)
	tablename := dm.TableName()

	err := dm.PreSave(conn)
	if err != nil {
		return toolkit.Error(fmt.Sprint("dbflex ", tablename+".PreSave ", err.Error()))
	}

	err = checkFK(conn, dm)
	if err != nil {
		return toolkit.Error(fmt.Sprint("dbflex ", tablename+".FK ", err.Error()))
	}

	_, err = conn.Execute(
		dbflex.From(tablename).Insert(),
		toolkit.M{}.Set("data", dm))

	if err == nil {
		err = dm.PostSave(conn)
		if err != nil {
			return toolkit.Error(fmt.Sprint("dbflex ", tablename+".PostSave ", err.Error()))
		}
	}
	return err
}

// Save will check if given DataModel filter is exist in the table or not
// If exist then update the data if not insert new data
func Save(conn dbflex.IConnection, dm DataModel) error {
	dm.SetThis(dm)
	tablename := dm.TableName()
	filter := generateFilterFromDataModel(conn, dm)

	dmexist := toolkit.M{}
	cursor := conn.Cursor(dbflex.From(tablename).Select().Where(filter), nil)
	errexist := cursor.Fetch(&dmexist).Close()

	err := dm.PreSave(conn)
	if err != nil {
		return toolkit.Error(fmt.Sprint("dbflex ", tablename+".PreSave ", err.Error()))
	}

	err = checkFK(conn, dm)
	if err != nil {
		return toolkit.Error(fmt.Sprint("dbflex ", tablename+".FK ", err.Error()))
	}

	if errexist == nil {
		_, err = conn.Execute(dbflex.From(tablename).Where(filter).Update(),
			toolkit.M{}.Set("data", dm).Set("singleupdate", true))
	} else {
		_, err = conn.Execute(dbflex.From(tablename).Insert(),
			toolkit.M{}.Set("data", dm))
	}

	if err == nil {
		err = dm.PostSave(conn)
		if err != nil {
			return toolkit.Error(fmt.Sprint("dbflex ", tablename+".PostSave ", err.Error()))
		}
	}

	err = updateReverseFK(conn, dm)
	if err != nil {
		return toolkit.Error(fmt.Sprint("dbflex ", tablename+".UpdReverseFK ", err.Error()))
	}

	return err
}

// Update  data from given connection and DataModel,
// filter is generated from given DataModel
func Update(conn dbflex.IConnection, dm DataModel) error {
	dm.SetThis(dm)
	tablename := dm.TableName()
	filter := generateFilterFromDataModel(conn, dm)

	err := dm.PreSave(conn)
	if err != nil {
		return toolkit.Error(fmt.Sprint("dbflex ", tablename+".PreSave ", err.Error()))
	}

	err = checkFK(conn, dm)
	if err != nil {
		return toolkit.Error(fmt.Sprint("dbflex ", tablename+".FK ", err.Error()))
	}

	_, err = conn.Execute(
		dbflex.From(tablename).Where(filter).Update(),
		toolkit.M{}.Set("data", dm).Set("singleupdate", true))

	if err == nil {
		err = dm.PostSave(conn)
		if err != nil {
			return toolkit.Error(fmt.Sprint("dbflex ", tablename+".PostSave ", err.Error()))
		}
	}

	err = updateReverseFK(conn, dm)
	if err != nil {
		return toolkit.Error(fmt.Sprint("dbflex ", tablename+".UpdReverseFK ", err.Error()))
	}

	return err
}

// Delete data from given connection and DataModel,
// filter is generated from given DataModel
func Delete(conn dbflex.IConnection, dm DataModel) error {
	dm.SetThis(dm)
	tablename := dm.TableName()
	filter := generateFilterFromDataModel(conn, dm)

	err := dm.PreDelete(conn)
	if err != nil {
		return toolkit.Error(fmt.Sprint("dbflex ", tablename+".PreDelete ", err.Error()))
	}

	err = checkEmptyFK(conn, dm)
	if err != nil {
		return toolkit.Error(fmt.Sprint("dbflex ", tablename+".PreDelete ", err.Error()))
	}

	_, err = conn.Execute(dbflex.From(tablename).Where(filter).Delete(), nil)

	if err == nil {
		err = dm.PostDelete(conn)
		if err != nil {
			return toolkit.Error(fmt.Sprint("dbflex ", tablename+".PostDelete ", err.Error()))
		}
	}

	return err
}

func generateFilterFromDataModel(conn dbflex.IConnection, dm DataModel) *dbflex.Filter {
	fields, values := dm.GetID(conn)
	if len(fields) == 0 {
		return new(dbflex.Filter)
	}

	fieldNameTag := conn.FieldNameTag()
	useTag := fieldNameTag != ""

	if useTag {
		vt := reflect.Indirect(reflect.ValueOf(dm)).Type()

		eqs := []*dbflex.Filter{}
		for idx, field := range fields {
			if f, ok := vt.FieldByName(field); ok {
				if tag, ok := f.Tag.Lookup(fieldNameTag); ok {
					eqs = append(eqs, dbflex.Eq(strings.Split(tag, ",")[0], values[idx]))
				} else {
					eqs = append(eqs, dbflex.Eq(field, values[idx]))
				}
			} else {
				eqs = append(eqs, dbflex.Eq(field, values[idx]))
			}
		}

		if len(eqs) == 1 {
			return eqs[0]
		}

		return dbflex.And(eqs...)
	}

	if len(fields) == 1 {
		return dbflex.Eq(fields[0], values[0])
	}

	eqs := []*dbflex.Filter{}
	for idx, field := range fields {
		eqs = append(eqs, dbflex.Eq(field, values[idx]))
	}
	return dbflex.And(eqs...)
}

func GetFieldName(obj interface{}, name, tag string, conn dbflex.IConnection) string {
	if tag == "" {
		tag = conn.FieldNameTag()
	}

	v := reflect.ValueOf(obj)
	if v.Kind() == reflect.Ptr {
		v = v.Elem()
	}
	t := v.Type()

	if f, ok := t.FieldByName(name); !ok {
		return ""
	} else {
		if tag == "" {
			return f.Name
		}
		if tagName := f.Tag.Get(tag); tagName == "" {
			return f.Name
		} else {
			return tagName
		}
	}
}
