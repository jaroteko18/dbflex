package orm

import (
	"errors"
	"fmt"

	"git.kanosolution.net/kano/dbflex"
	"github.com/eaciit/toolkit"
)

type FKConfig struct {
	FieldID          string
	RefTableName     string
	RefField         string
	RefDefaultFields []string
	AutoCreate       bool
	Map              toolkit.M
}

type ReverseFKConfig struct {
	FieldID      string
	RefTableName string
	RefField     string
	AutoDelete   bool
	Map          toolkit.M
}

func checkFK(conn dbflex.IConnection, dm DataModel) error {
	fks := dm.FK()
	for _, fk := range fks {
		if e := ensureFK(conn, dm, fk.FieldID, fk.RefTableName, fk.RefField, fk.AutoCreate, fk.Map, fk.RefDefaultFields...); e != nil {
			return e
		}
	}
	return nil
}

func updateReverseFK(conn dbflex.IConnection, dm DataModel) error {
	fks := dm.ReverseFK()
	mSource, e := toolkit.ToM(dm.This())
	if e != nil {
		return fmt.Errorf("reverseFKErr: %s, %s", dm.TableName(), e.Error())
	}

	for _, fk := range fks {
		val := mSource.Get(fk.FieldID)
		if e != nil {
			return errors.New("fkErr: " + fk.RefTableName)
		}
		fields := make([]string, len(fk.Map))
		if len(fields) > 0 {
			mUpdate := toolkit.M{}
			fieldIdx := 0
			for k, v := range fk.Map {
				fields[fieldIdx] = v.(string)
				updVal := mSource.Get(k)
				mUpdate.Set(fields[fieldIdx], updVal)
				fieldIdx++
			}
			cmd := dbflex.From(fk.RefTableName).Where(dbflex.Eq(fk.RefField, val)).Update(fields...)
			if _, e := conn.Execute(cmd, toolkit.M{}.Set("data", mUpdate)); e != nil {
				return fmt.Errorf("fkErr: %s, %s", fk.RefTableName, e.Error())
			}
		}
	}
	return nil
}

func checkEmptyFK(conn dbflex.IConnection, dm DataModel) error {
	fks := dm.ReverseFK()

	for _, fk := range fks {
		if e := ensureEmptyFK(conn, dm, fk.FieldID, fk.RefTableName, fk.RefField, fk.AutoDelete); e != nil {
			return e
		}
	}
	return nil
}
