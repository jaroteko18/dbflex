package dbflex

import (
	"strings"

	"github.com/eaciit/toolkit"
)

// QueryItems map of QeuryItem
type QueryItems map[string]QueryItem

// ICommand is interface abstraction fo all command should be supported by each driver
type ICommand interface {
	Reset() ICommand
	Select(...string) ICommand
	From(string) ICommand
	Where(*Filter) ICommand
	OrderBy(...string) ICommand
	GroupBy(...string) ICommand

	Aggr(...*AggrItem) ICommand
	Insert(...string) ICommand
	Update(...string) ICommand
	Delete() ICommand
	Save() ICommand

	Take(int) ICommand
	Skip(int) ICommand

	SetAttr(string, interface{}) ICommand
	Attr(string, interface{}) interface{}
	HasAttr(string) (bool, interface{})

	Command(interface{}) ICommand
	SQL(string) ICommand
	SetItems(items QueryItems) ICommand
	Items() QueryItems
}

// CommandBase is base struct for any struct that implement ICommand for ease of implementation
type CommandBase struct {
	//items []*QueryItem
	items QueryItems
	m     toolkit.M
}

// SetItems set items value
func (b *CommandBase) SetItems(items QueryItems) ICommand {
	b.items = items
	return b
}

func (b *CommandBase) Items() QueryItems {
	if b.items == nil {
		b.items = QueryItems{}
	}
	return b.items
}

// Reset base implementation of Reset method
func (b *CommandBase) Reset() ICommand {
	b.items = QueryItems{}
	return b
}

// Select base implementation of Select method
func (b *CommandBase) Select(fields ...string) ICommand {
	b.items[QuerySelect] = QueryItem{QuerySelect, fields}
	return b
}

// From base implementation of From method
func (b *CommandBase) From(name string) ICommand {
	if b.items == nil {
		b.items = QueryItems{}
	}
	b.m = toolkit.M{}
	b.items[QueryFrom] = QueryItem{QueryFrom, name}
	return b
}

// Where base implementation of Where method
func (b *CommandBase) Where(f *Filter) ICommand {
	b.items[QueryWhere] = QueryItem{QueryWhere, f}
	return b
}

// OrderBy base implementation of OrderBy method
func (b *CommandBase) OrderBy(fields ...string) ICommand {
	b.items[QueryOrder] = QueryItem{QueryOrder, fields}
	return b
}

// GroupBy base implementation of GroupBy method
func (b *CommandBase) GroupBy(fields ...string) ICommand {
	b.items[QueryGroup] = QueryItem{QueryGroup, fields}
	return b
}

// Aggr base implementation of Aggr method
func (b *CommandBase) Aggr(aggritems ...*AggrItem) ICommand {
	b.items[QueryAggr] = QueryItem{QueryAggr, aggritems}
	return b
}

// Insert base implementation of Insert method
func (b *CommandBase) Insert(fields ...string) ICommand {
	b.items[QueryInsert] = QueryItem{QueryInsert, fields}
	return b
}

// Update base implementation of Update method
func (b *CommandBase) Update(fields ...string) ICommand {
	b.items[QueryUpdate] = QueryItem{QueryUpdate, fields}
	return b
}

// Delete base implementation of Delete method
func (b *CommandBase) Delete() ICommand {
	b.items[QueryDelete] = QueryItem{QueryDelete, true}
	return b
}

// Save base implementation of Save method
func (b *CommandBase) Save() ICommand {
	b.items[QuerySave] = QueryItem{QuerySave, true}
	return b
}

// Take base implementation of Take method
func (b *CommandBase) Take(n int) ICommand {
	b.items[QueryTake] = QueryItem{QueryTake, n}
	return b
}

// Skip base implementation of Skip method
func (b *CommandBase) Skip(n int) ICommand {
	b.items[QuerySkip] = QueryItem{QuerySkip, n}
	return b
}

// Command base implementation of Command method
func (b *CommandBase) Command(command interface{}) ICommand {
	b.items[QueryCommand] = QueryItem{QueryCommand, command}
	return b
}

// SQL base implementation of SQL method
func (b *CommandBase) SQL(sql string) ICommand {
	if b.items == nil {
		b.items = QueryItems{}
	}
	b.m = toolkit.M{}
	b.items[QuerySQL] = QueryItem{QuerySQL, sql}
	return b
}

// SetAttr set attribute value
func (b *CommandBase) SetAttr(key string, value interface{}) ICommand {
	b.m[key] = value
	return b
}

// Attr get attribute value and if none, assign default
func (b *CommandBase) Attr(key string, def interface{}) interface{} {
	v, has := b.m[key]
	if !has {
		return def
	}
	return v
}

// HasAttr check if command has attr and get the valye
func (b *CommandBase) HasAttr(key string) (bool, interface{}) {
	v, has := b.m[key]
	return has, v
}

func CopyCommand(c ICommand) ICommand {
	cb := new(CommandBase)
	items := c.Items()
	qis := QueryItems{}
	for k, v := range items {
		switch k {
		case QueryWhere:
			if v.Value != nil {
				if f, ok := v.Value.(*Filter); ok {
					v.Value = CopyFilter(f)
				}
			}

		default:
			qis[k] = v
		}
	}
	cb.SetItems(qis)
	return cb
}

func CopyFilter(f *Filter) *Filter {
	copy := new(Filter)
	if copy.Op == OpAnd || copy.Op == OpOr {
		copied := []*Filter{}
		for _, item := range copy.Items {
			copied = append(copied, CopyFilter(item))
		}
		copy.Items = copied
	} else {
		*copy = *f
	}
	return copy
}

func PushVarToCommand(c ICommand, vars toolkit.M) {
	items := c.Items()
	qis := QueryItems{}
	for k, v := range items {
		switch k {
		case QueryWhere:
			if v.Value != nil {
				if f, ok := v.Value.(*Filter); ok {
					PushVarToFilter(f, vars)
				}
			}

		default:
			qis[k] = v
		}
	}
	c.SetItems(qis)
}

func PushVarToFilter(f *Filter, vars toolkit.M) {
	if f.Op == OpAnd || f.Op == OpOr {
		copied := []*Filter{}
		for _, item := range f.Items {
			PushVarToFilter(item, vars)
			copied = append(copied, item)
		}
		f.Items = copied
	} else {
		for k, v := range vars {
			if vs, ok := f.Value.(string); ok {
				if vs == "%"+k {
					f.Value = v
				} else if strings.Contains(vs, "%"+k) {
					if varTxt, ok := v.(string); ok {
						f.Value = strings.Replace(vs, "%"+k, varTxt, -1)
					}
				}
			}
		}
	}
}
