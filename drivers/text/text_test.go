package text

import (
	"fmt"
	"math"
	"math/rand"
	"os"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"git.kanosolution.net/kano/dbflex"

	"github.com/eaciit/toolkit"

	"git.kanosolution.net/kano/dbflex/testbase"
	. "github.com/smartystreets/goconvey/convey"
)

var (
	cfg = NewConfig(',').
		SetUseSign(true).SetSign('"').
		SetDateFormat("", "yyyy-MM-dd HH:mm:ss")
	workpath = os.TempDir()
)

func TestObjectHeader(t *testing.T) {
	Convey("Properly fetch object header", t, func() {
		type fakeModel struct {
			ID          string `flex:"id"`
			Title       string
			NumberInt   int
			NumberFloat float64
			Created     time.Time
		}

		fm := new(fakeModel)
		texts := objHeader(fm)
		text := strings.Join(texts, ",")

		toolkit.Logger().SetLevelStdOut(toolkit.DebugLevel, false)

		Convey("Data is valid", func() {
			validHeaders := strings.Split("ID,Title,NumberInt,NumberFloat,Created", ",")
			texts := strings.Split(text, ",")

			for _, validHeader := range validHeaders {
				So(texts, ShouldContain, validHeader)
			}
		})
	})
}

func TestObjectToText(t *testing.T) {
	Convey("Cast object to text", t, func() {
		type fakeModel struct {
			ID          string `flex:"id"`
			Title       string
			NumberInt   int
			NumberFloat float64
			Created     time.Time
		}

		fm := new(fakeModel)

		fm.ID = "TEST-1"
		fm.Title = "TEST-2"
		fm.NumberInt = 1
		fm.NumberFloat = 0.0001
		fm.Created = toolkit.ToDate("2018-07-10 00:00:00", "yyyy-MM-dd hh:mm:ss")

		text, err := objToText(fm, objHeader(fm), NewConfig(','))

		Convey("No error", func() { So(err, ShouldBeNil) })
		Convey("Data is valid", func() {
			So(text, ShouldContainSubstring, `"TEST-1"`)
			So(text, ShouldContainSubstring, `"TEST-2"`)
			So(text, ShouldContainSubstring, `1`)
			So(text, ShouldContainSubstring, `0.000100`)
			So(text, ShouldContainSubstring, `2018-07-10 00:00:00`)
		})
	})
}

func TestTextToObjField(t *testing.T) {
	Convey("Cast a text to object field", t, func() {
		type fakeModel struct {
			ID          string `flex:"id"`
			Title       string
			NumberInt   int
			NumberFloat float64
			Created     time.Time
		}

		fm := new(fakeModel)

		Convey("Set string", func() {
			err := processTxtToObjField("Record1", fm, "id", cfg)
			So(err, ShouldBeNil)
			Convey("Data is valid", func() { So(fm.ID, ShouldEqual, "Record1") })
		})

		Convey("Set float", func() {
			err := processTxtToObjField("30.5", fm, "numberfloat", cfg)
			So(err, ShouldBeNil)
			Convey("Data is valid", func() { So(fm.NumberFloat, ShouldEqual, 30.5) })
		})

		Convey("Set Date", func() {
			txt := "2018-06-01 00:00:00"
			dt := toolkit.ToDate(txt, "yyyy-MM-dd hh:mm:ss")
			err := processTxtToObjField(txt, fm, "created", cfg)
			So(err, ShouldBeNil)
			Convey("Data is valid", func() {
				diff := math.Abs(float64(fm.Created.Sub(dt)))
				So(diff, ShouldBeLessThanOrEqualTo, float64(1*time.Second))
			})
		})
	})
}

func TestTextToMapField(t *testing.T) {
	Convey("Cast a text to map field", t, func() {
		fm := toolkit.M{}

		Convey("Set string", func() {
			err := processTxtToObjField("Record1", fm, "ID", cfg)
			Convey("No error", func() { So(err, ShouldBeNil) })
			Convey("Data is valid", func() { So(fm.GetString("ID"), ShouldEqual, "Record1") })
		})

		Convey("Set float", func() {
			err := processTxtToObjField("30.5", fm, "NumberFloat", cfg)
			Convey("No error", func() { So(err, ShouldBeNil) })
			Convey("Data is valid", func() { So(fm.GetFloat64("NumberFloat"), ShouldEqual, 30.5) })
		})

		Convey("Set Date", func() {
			txt := "2018-06-01 00:00:00"
			dt := toolkit.ToDate(txt, "yyyy-MM-dd hh:mm:ss")
			err := processTxtToObjField(txt, fm, "Created", cfg)
			Convey("No error", func() { So(err, ShouldBeNil) })
			Convey("Data is valid", func() {
				diff := math.Abs(float64(fm.Get("Created", time.Now()).(time.Time).Sub(dt)))
				So(diff, ShouldBeLessThanOrEqualTo, float64(1*time.Second))
			})
		})
	})
}

func TestTextToObj(t *testing.T) {
	Convey("Cast a text to object", t, func() {
		type fakeModel struct {
			ID          string
			Title       string
			NumberInt   int
			NumberFloat float64
			Created     time.Time
		}

		data := "\"Record1\",\"Title untuk Record 1\",30,20.5,\"2018-06-15 10:00:00\""

		Convey("To Obj", func() {
			fm := new(fakeModel)
			err := textToObj(data, fm, cfg)
			Convey("No error", func() { So(err, ShouldBeNil) })
			Convey("Valid data", func() {
				So(fm.ID, ShouldEqual, "Record1")
				So(fm.NumberInt, ShouldEqual, 30)
				So(fm.NumberFloat, ShouldEqual, 20.5)

				date1txt := "2018-06-15 10:00:00"
				date2txt := toolkit.Date2String(fm.Created, cfg.DateFormat(""))
				So(date1txt, ShouldEqual, date2txt)
			})
		})

		Convey("To M", func() {
			m := toolkit.M{}
			err := textToObj(data, &m, cfg, "ID", "Title", "NumberInt", "NumberFloat", "Created")
			Convey("No error", func() { So(err, ShouldBeNil) })
			Convey("Valid data", func() {
				So(m.GetString("ID"), ShouldEqual, "Record1")
				So(m.GetInt("NumberInt"), ShouldEqual, 30)

				float := m.GetFloat32("NumberFloat")
				So(float, ShouldEqual, 20.5)

				date1txt := "2018-06-15 10:00:00"
				date2txt := toolkit.Date2String(m.Get("Created", time.Now()).(time.Time), cfg.DateFormat(""))
				So(date1txt, ShouldEqual, date2txt)
			})
		})

		Convey("Open quote without closing", func() {
			txt := "makan'ikan,asin,sekali"
			out := toolkit.M{}
			err := textToObj(txt, &out, cfg, "A", "B", "C")
			So(err, ShouldBeNil)
			Convey("Data is valid", func() {
				So(out["A"], ShouldEqual, "makan'ikan")
				So(out["B"], ShouldEqual, "asin")
				So(out["C"], ShouldEqual, "sekali")
			})
		})
	})
}

func TestPartialInsert(t *testing.T) {
	Convey("Partial Insert", t, func() {
		type FakeModel struct {
			ID       string `sql:"_id"`
			Name     string
			Grade    int
			Role     string
			JoinDate time.Time
		}

		tableName := "employees-partial"

		fm := new(FakeModel)
		fm.ID = "TEST-PARTIAL-OBJECT-1"
		fm.Name = "Test partial object 1"
		fm.Grade = 1
		fm.JoinDate = time.Now()
		fm.Role = "Manager"

		config := NewConfig(',')
		config.WriteMode = ModeLoose

		conn, err := dbflex.NewConnectionFromURI(toolkit.Sprintf("text://localhost/%s?extension=csv&separator=comma", workpath), toolkit.M{}.Set("text_obj_setting", config))
		So(err, ShouldBeNil)
		So(conn, ShouldNotBeNil)

		err = conn.Connect()
		So(err, ShouldBeNil)

		Convey("Insert data object", func() {
			//isErr := false
			query, err := conn.Prepare(dbflex.From(tableName).Insert())
			Convey("Prepare insert command", func() {
				So(err, ShouldBeNil)
			})

			Convey("Iterating insert command", func() {
				_, err := query.Execute(toolkit.M{}.Set("data", fm))
				So(err, ShouldBeNil)
			})
		})

		em := toolkit.M{
			"Name":     "Test partial M 1",
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
	})
}

func TestPartialInsertNested(t *testing.T) {
	Convey("Partial Insert", t, func() {
		type FakeName struct {
			First string
			Last  string
		}
		type FakeModel struct {
			ID       string `sql:"_id"`
			FullName FakeName
			Grade    int
			Role     string
			JoinDate time.Time
		}

		tableName := "employees-partial-nested"

		fm := new(FakeModel)
		fm.ID = "TEST-PARTIAL-OBJECT-1"
		fm.FullName = FakeName{"Bagus", "Cahyono"}
		fm.Grade = 1
		fm.JoinDate = time.Now()
		fm.Role = "Manager"

		config := NewConfig(',')
		config.WriteMode = ModeLoose

		conn, err := dbflex.NewConnectionFromURI(toolkit.Sprintf("text://localhost/%s?extension=csv&separator=comma", workpath), toolkit.M{}.Set("text_obj_setting", config))
		So(err, ShouldBeNil)
		So(conn, ShouldNotBeNil)

		err = conn.Connect()
		So(err, ShouldBeNil)

		Convey("Insert data object", func() {
			//isErr := false
			query, err := conn.Prepare(dbflex.From(tableName).Insert())
			Convey("Prepare insert command", func() {
				So(err, ShouldBeNil)
			})

			Convey("Iterating insert command", func() {
				_, err := query.Execute(toolkit.M{}.Set("data", fm))
				So(err, ShouldBeNil)
			})

			Convey("Get eq data object", func() {
				buffer := []FakeModel{}
				cmd := dbflex.From(tableName).Select().Where(dbflex.Eq("_id", fm.ID))
				conn.Cursor(cmd, nil).Fetchs(&buffer, 0)
				So(buffer[0].FullName.First, ShouldEqual, fm.FullName.First)
				So(buffer[0].FullName.Last, ShouldEqual, fm.FullName.Last)
			})
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
			So(buffer[0].Get("FullName").(toolkit.M).Get("First"), ShouldEqual, "Bagus")
			So(buffer[0].Get("FullName").(toolkit.M).Get("Last"), ShouldEqual, "Cahyono")
		})
	})
}

func TestMultipleInsert(t *testing.T) {
	Convey("Multiple partial insert", t, func() {
		type FakeModel struct {
			ID       string `sql:"_id"`
			Name     string
			Grade    int
			Role     string
			JoinDate time.Time
		}

		config := NewConfig(',')
		config.WriteMode = ModeLoose

		conn, err := dbflex.NewConnectionFromURI(toolkit.Sprintf("text://localhost/%s?extension=csv&separator=comma", workpath), toolkit.M{}.Set("text_obj_setting", config))
		So(err, ShouldBeNil)
		So(conn, ShouldNotBeNil)

		err = conn.Connect()
		So(err, ShouldBeNil)

		Convey("Clear table", func() {
			_, err := conn.Execute(dbflex.From("employees-multiple").Delete(), nil)
			So(err, ShouldBeNil)
		})

		fms := []*FakeModel{}
		for i := 0; i < 1000; i++ {
			fm := new(FakeModel)
			fm.ID = fmt.Sprintf("TEST-PARTIAL-OBJECT-%v", i)
			fm.Name = fmt.Sprintf("Test partial object %v", i)
			fm.Grade = i
			fm.JoinDate = time.Now()
			fm.Role = "Manager"

			fms = append(fms, fm)
		}

		wg := new(sync.WaitGroup)

		Convey("Insert data object", func() {
			query, err := conn.Prepare(dbflex.From("employees-multiple").Insert())
			So(err, ShouldBeNil)

			for _, fm := range fms {
				wg.Add(1)

				go func(f *FakeModel) {
					_, _ = query.Execute(toolkit.M{}.Set("data", f))
					wg.Done()
				}(fm)
			}
		})

		wg.Wait()

		Convey("Check result", func() {
			for _, fm := range fms {
				cmd := dbflex.From("employees-multiple").Select().Where(dbflex.Eq("_id", fm.ID))
				n := conn.Cursor(cmd, nil).Count()
				So(err, ShouldBeNil)
				So(n, ShouldEqual, 1)
			}
		})
	})
}

func TestMultipleUpdate(t *testing.T) {
	Convey("Multiple partial update", t, func() {
		type FakeModel struct {
			ID   string `sql:"_id"`
			Name string
		}

		config := NewConfig(',')
		config.WriteMode = ModeLoose

		conn, err := dbflex.NewConnectionFromURI(toolkit.Sprintf("text://localhost/%s?extension=csv&separator=comma", workpath), toolkit.M{}.Set("text_obj_setting", config))
		So(err, ShouldBeNil)
		So(conn, ShouldNotBeNil)

		err = conn.Connect()
		So(err, ShouldBeNil)

		r := rand.New(rand.NewSource(0))
		fms := []*FakeModel{}
		for i := 0; i < 500; i++ {
			j := r.Intn(500)
			fm := new(FakeModel)
			fm.ID = fmt.Sprintf("TEST-PARTIAL-OBJECT-%v", j)
			fm.Name = fmt.Sprintf("NEW Test partial object %v", j)

			fms = append(fms, fm)
		}

		wg := new(sync.WaitGroup)

		Convey("Update data object", func() {
			for _, fm := range fms {
				wg.Add(1)

				go func(f *FakeModel) {
					_, err = conn.Execute(dbflex.From("employees-multiple").Where(dbflex.Eq("_id", f.ID)).Update("Name"), toolkit.M{}.Set("data", f))
					if err != nil {
						fmt.Println(err)
					}
					wg.Done()
				}(fm)
			}
		})

		wg.Wait()

		Convey("Check result", func() {
			buffer := []FakeModel{}

			for _, fm := range fms {
				cmd := dbflex.From("employees-multiple").Select().Where(dbflex.Eq("_id", fm.ID))
				err := conn.Cursor(cmd, nil).Fetchs(&buffer, 0).Error()
				So(err, ShouldBeNil)
				So(len(buffer), ShouldEqual, 1)
				So(buffer[0].Name, ShouldEqual, fm.Name)
			}
		})
	})
}

func TestMultipleDelete(t *testing.T) {
	Convey("Multiple partial delete", t, func() {
		config := NewConfig(',')
		config.WriteMode = ModeLoose

		conn, err := dbflex.NewConnectionFromURI(toolkit.Sprintf("text://localhost/%s?extension=csv&separator=comma", workpath), toolkit.M{}.Set("text_obj_setting", config))
		So(err, ShouldBeNil)
		So(conn, ShouldNotBeNil)

		err = conn.Connect()
		So(err, ShouldBeNil)

		r := rand.New(rand.NewSource(0))
		ids := []string{}
		for i := 0; i < 500; i++ {
			j := r.Intn(500)
			id := fmt.Sprintf("TEST-PARTIAL-OBJECT-%v", j)
			ids = append(ids, id)
		}

		wg := new(sync.WaitGroup)

		Convey("Delete data object", func() {
			for _, id := range ids {
				wg.Add(1)

				go func(f string) {
					_, err = conn.Execute(dbflex.From("employees-multiple").Where(dbflex.Eq("_id", f)).Delete(), nil)
					wg.Done()
				}(id)
			}
		})

		wg.Wait()

		Convey("Check result", func() {
			for _, id := range ids {
				cmd := dbflex.From("employees-multiple").Select().Where(dbflex.Eq("_id", id))
				n := conn.Cursor(cmd, nil).Count()
				So(err, ShouldBeNil)
				So(n, ShouldEqual, 0)
			}
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

		tableName := "employees_array"

		config := NewConfig(',')
		config.WriteMode = ModeLoose

		conn, err := dbflex.NewConnectionFromURI(toolkit.Sprintf("text://localhost/%s?extension=csv&separator=comma", workpath), toolkit.M{}.Set("text_obj_setting", config))
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
		conn, err := dbflex.NewConnectionFromURI(toolkit.Sprintf("text://localhost/%s?extension=csv&separator=comma", workpath), nil)
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
	crud := testbase.NewCRUD(t, toolkit.Sprintf("text://localhost/%s?extension=csv&separator=comma", workpath),
		1000,
		toolkit.M{}.Set("conn_config", toolkit.M{}.Set("text_obj_setting", cfg)))
	crud.RunTest()

	Convey("Sorting on M", t, func() {
		conn, err := dbflex.NewConnectionFromURI(toolkit.Sprintf("text://localhost/%s?extension=csv&separator=comma", workpath), nil)
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

		Convey("Aggregate group by", func() {
			Convey("Get aggregation M", func() {
				buffer1 := []toolkit.M{}
				cmd := dbflex.From("employees").Select().Where(dbflex.Eq("Grade", 3)).Aggr(dbflex.Sum("Salary")).GroupBy("grade")
				err := conn.Cursor(cmd, nil).Fetchs(&buffer1, 0).Error()
				So(err, ShouldBeNil)

				buffer2 := []struct {
					Grade  int
					Salary int
				}{}
				cmd = dbflex.From("employees").Select().Where(dbflex.Eq("Grade", 3)).Aggr(dbflex.Sum("Salary")).GroupBy("grade")
				err = conn.Cursor(cmd, nil).Fetchs(&buffer2, 0).Error()
				So(err, ShouldBeNil)
				So(buffer1[0]["Salary"].(float64), ShouldEqual, float64(buffer2[0].Salary))
			})
		})
	})
}
