package json

import (
	"os"
	"strconv"
	"testing"
	"time"

	"git.kanosolution.net/kano/dbflex"
	"git.kanosolution.net/kano/dbflex/testbase"
	"github.com/eaciit/toolkit"
	. "github.com/smartystreets/goconvey/convey"
)

var workpath = os.TempDir()

func TestPartialInsertNested(t *testing.T) {
	Convey("Partial Insert", t, func() {
		type FakeName struct {
			First string
			Last  string
		}
		type FakeModel struct {
			ID       string `json:"_id"`
			FullName FakeName
			Grade    int
			Role     string
			JoinDate time.Time
		}

		tableName := "employees2"

		conn, err := dbflex.NewConnectionFromURI(toolkit.Sprintf("json://localhost/%s?extension=json", workpath), toolkit.M{})
		So(err, ShouldBeNil)
		So(conn, ShouldNotBeNil)

		err = conn.Connect()
		So(err, ShouldBeNil)

		Convey("Clear data", func() {
			conn.Execute(dbflex.From(tableName).Delete(), nil)
			So(err, ShouldBeNil)
		})

		fm := new(FakeModel)
		fm.ID = "TEST-PARTIAL-OBJECT-1"
		fm.FullName = FakeName{"Bagus", "Cahyono"}
		fm.Grade = 1
		fm.JoinDate = time.Now()
		fm.Role = "Manager"

		Convey("Insert data object", func() {
			query, err := conn.Prepare(dbflex.From(tableName).Insert())
			Convey("Prepare insert command", func() {
				So(err, ShouldBeNil)
			})

			Convey("Iterating insert command", func() {
				_, err := query.Execute(toolkit.M{}.Set("data", fm))
				So(err, ShouldBeNil)
			})
		})

		Convey("Get eq data object", func() {
			buffer := []FakeModel{}
			cmd := dbflex.From(tableName).Select().Where(dbflex.Eq("_id", fm.ID))
			conn.Cursor(cmd, nil).Fetchs(&buffer, 0)
			So(buffer[0].FullName.First, ShouldEqual, fm.FullName.First)
			So(buffer[0].FullName.Last, ShouldEqual, fm.FullName.Last)
		})

		em := toolkit.M{
			"FullName": toolkit.M{
				"First": "Bagus",
				"Last":  "Cahyono",
			},
			"grade":    2,
			"_id":      "TEST-PARTIAL-M-1",
			"joinDate": time.Now(),
			"Role":     "Owner",
		}

		Convey("Insert data M", func() {
			//isErr := false
			query, err := conn.Prepare(dbflex.From(tableName).Insert())
			Convey("Prepare insert command", func() {
				So(err, ShouldBeNil)
			})

			Convey("Iterating insert command", func() {
				_, err := query.Execute(toolkit.M{}.Set("data", em))
				So(err, ShouldBeNil)
			})
		})

		Convey("Get eq data M", func() {
			buffer := []toolkit.M{}
			cmd := dbflex.From(tableName).Select().Where(dbflex.Eq("_id", em["_id"]))
			conn.Cursor(cmd, nil).Fetchs(&buffer, 0)
			So(buffer[0].Get("FullName").(map[string]interface{})["First"].(string), ShouldEqual, "Bagus")
			So(buffer[0].Get("FullName").(map[string]interface{})["Last"].(string), ShouldEqual, "Cahyono")
		})

		Convey("Nested filter", func() {
			buffer := []FakeModel{}
			cmd := dbflex.From(tableName).Select().Where(dbflex.Eq("FullName.First", fm.FullName.First))
			conn.Cursor(cmd, nil).Fetchs(&buffer, 0)
			So(buffer[0].FullName.First, ShouldEqual, fm.FullName.First)
			So(buffer[0].FullName.Last, ShouldEqual, fm.FullName.Last)
		})
	})
}

func TestInsertArray(t *testing.T) {
	Convey("Partial Insert", t, func() {
		type FakeName struct {
			First string
			Last  string
		}
		type FakeModel struct {
			ID       string `json:"_id"`
			FullName FakeName
			Grade    int
			Role     string
			JoinDate time.Time
		}

		tableName := "employees3"

		conn, err := dbflex.NewConnectionFromURI(toolkit.Sprintf("json://localhost/%s?extension=json", workpath), toolkit.M{})
		So(err, ShouldBeNil)
		So(conn, ShouldNotBeNil)

		err = conn.Connect()
		So(err, ShouldBeNil)

		Convey("Clear data", func() {
			conn.Execute(dbflex.From(tableName).Delete(), nil)
			So(err, ShouldBeNil)
		})

		fms := []*FakeModel{}

		for i := 0; i < 10; i++ {
			fm := new(FakeModel)
			fm.ID = "TEST-PARTIAL-OBJECT-" + strconv.Itoa(i)
			fm.FullName = FakeName{"Bagus", "Cahyono"}
			fm.Grade = i
			fm.JoinDate = time.Now()
			fm.Role = "Manager"

			fms = append(fms, fm)
		}

		Convey("Insert multiple data object", func() {
			query, err := conn.Prepare(dbflex.From(tableName).Insert())
			Convey("Prepare insert command", func() {
				So(err, ShouldBeNil)
			})

			Convey("Iterating insert command", func() {
				_, err := query.Execute(toolkit.M{}.Set("data", fms))
				So(err, ShouldBeNil)
			})
		})

		Convey("Validate", func() {
			buffer := []toolkit.M{}
			cmd := dbflex.From(tableName).Select()
			conn.Cursor(cmd, nil).Fetchs(&buffer, 0)
			So(len(buffer), ShouldEqual, len(fms))
		})
	})
}

func TestSaveCommand(t *testing.T) {
	Convey("Save command", t, func() {
		tableName := "employees-save"
		conn, err := dbflex.NewConnectionFromURI(toolkit.Sprintf("json://localhost/%s?extension=json", workpath), toolkit.M{})
		So(err, ShouldBeNil)
		So(conn, ShouldNotBeNil)

		err = conn.Connect()
		So(err, ShouldBeNil)

		Convey("Clear data", func() {
			conn.Execute(dbflex.From(tableName).Delete(), nil)
			So(err, ShouldBeNil)
		})

		saveCmd := dbflex.From(tableName).Save()
		_, err = conn.Execute(saveCmd, toolkit.M{}.Set("data", toolkit.M{}.Set("_id", "BAGUS").Set("Value", "Believe Me")))
		So(err, ShouldBeNil)

		buffer := []toolkit.M{}
		cmd := dbflex.From(tableName).Where(dbflex.Eq("_id", "BAGUS"))
		err = conn.Cursor(cmd, nil).Fetchs(&buffer, 0).Error()
		So(err, ShouldBeNil)
		So(buffer[0]["Value"], ShouldEqual, "Believe Me")

		_, err = conn.Execute(saveCmd, toolkit.M{}.Set("data", toolkit.M{}.Set("_id", "BAGUS").Set("Value", "Believe You")))
		So(err, ShouldBeNil)

		cmd = dbflex.From(tableName).Where(dbflex.Eq("_id", "BAGUS"))
		err = conn.Cursor(cmd, nil).Fetchs(&buffer, 0).Error()
		So(err, ShouldBeNil)
		So(buffer[0]["Value"], ShouldEqual, "Believe You")

		_, err = conn.Execute(saveCmd, toolkit.M{}.Set("data", toolkit.M{}.Set("_id", "CAHYONO").Set("Value", "Believe Us")))
		So(err, ShouldBeNil)

		cmd = dbflex.From(tableName).Where(dbflex.Eq("_id", "CAHYONO"))
		err = conn.Cursor(cmd, nil).Fetchs(&buffer, 0).Error()
		So(err, ShouldBeNil)
		So(buffer[0]["Value"], ShouldEqual, "Believe Us")
	})
}
func TestCRUD(t *testing.T) {
	crud := testbase.NewCRUD(t, toolkit.Sprintf("json://localhost/%s?extension=json", workpath), 1000, toolkit.M{})
	crud.RunTest()

	Convey("Sorting on M", t, func() {
		conn, err := dbflex.NewConnectionFromURI(toolkit.Sprintf("json://localhost/%s?extension=json", workpath), toolkit.M{})
		So(err, ShouldBeNil)
		So(conn, ShouldNotBeNil)

		err = conn.Connect()
		So(err, ShouldBeNil)

		buffer := []toolkit.M{}
		cmd := dbflex.From("employees").OrderBy("Grade")
		err = conn.Cursor(cmd, nil).Fetchs(&buffer, 0).Error()

		So(err, ShouldBeNil)
		So(len(buffer), ShouldNotEqual, 0)

		for i := 0; i < len(buffer)-1; i++ {
			So(buffer[i].GetInt("Grade"), ShouldBeLessThanOrEqualTo, buffer[i+1].GetInt("Grade"))
		}
	})
}
