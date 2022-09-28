package orm

import (
	"errors"

	"git.kanosolution.net/kano/dbflex"
	"github.com/ariefdarmawan/reflector"
	"github.com/eaciit/toolkit"
)

func ensureFK(hub dbflex.IConnection, dm DataModel, fieldID, sourceTableName, sourceField string, create bool, refMap toolkit.M, defaultFields ...string) error {
	rf := reflector.From(dm)
	keyValue, e := rf.Get(fieldID)
	if e != nil {
		return errors.New("fkErr: " + sourceTableName + ", " + e.Error())
	}

	if keyValue != "" {
		mRef := toolkit.M{}
		cmdGetRef := dbflex.From(sourceTableName).Where(dbflex.Eq(sourceField, keyValue)).Select().Take(1)
		if e = hub.Cursor(cmdGetRef, nil).Fetch(&mRef).Error(); e != nil {
			if create {
				cmdSave := dbflex.From(sourceTableName).Save()
				mRef.Set(sourceField, keyValue)
				for _, def := range defaultFields {
					mRef.Set(def, keyValue)
				}
				if _, eSave := hub.Execute(cmdSave, toolkit.M{}.Set("data", mRef)); eSave != nil {
					return errors.New("fkErr: " + sourceTableName + ", " + eSave.Error())
				}
			} else {
				return errors.New("missingFK: " + sourceTableName)
			}
		} else {
			if refMap != nil {
				for field, source := range refMap {
					rf.Set(field, mRef.Get(source.(string)))
				}
				rf.Flush()
			}
		}
	}

	return nil
}

func ensureEmptyFK(hub dbflex.IConnection, dm DataModel, fieldID, refTableName, refField string, autoDel bool) error {
	sourceM, e := toolkit.ToM(dm)
	if e != nil {
		return errors.New("fkErr: " + refTableName)
	}
	keyValue := sourceM.GetString(fieldID)

	if keyValue != "" {
		cmd := dbflex.From(refTableName).Where(dbflex.Eq(refField, keyValue)).Select().Take(1)
		refM := toolkit.M{}
		if e = hub.Cursor(cmd, nil).Fetch(&refM).Error(); e == nil {
			if !autoDel {
				return errors.New("fkIsNotEmpty: " + refTableName)
			}

			cmdDel := dbflex.From(refTableName).Where(dbflex.Eq(refField, keyValue)).Delete()
			if _, e := hub.Execute(cmdDel, nil); e != nil {
				return errors.New("fkAutoDeleteErr: " + refTableName + ", " + e.Error())
			}
		}
	}
	return nil
}
