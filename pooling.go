package dbflex

import (
	"fmt"
	"sync"
	"time"

	"github.com/eaciit/toolkit"
)

var (
	DefaultPoolingTimeout = 30 * time.Second
)

// DbPooling is database pooling system in dbflex
type DbPooling struct {
	sync.RWMutex
	size  int
	items []*PoolItem
	fnNew func() (IConnection, error)

	// Timeout max time required to obtain new connection
	Timeout time.Duration

	// AutoRelease defines max time for a connection to be auto released after it is being idle. 0 = no autorelease (default)
	AutoRelease time.Duration

	// AutoClose defines max time for a connection to be autoclosed after it is being idle. 0 = no auto close (default)
	AutoClose time.Duration

	_log *toolkit.LogEngine
}

func (h *DbPooling) Log() *toolkit.LogEngine {
	if h._log == nil {
		h._log = toolkit.NewLogEngine(true, false, "", "", "")
	}
	return h._log
}

func (h *DbPooling) SetLog(l *toolkit.LogEngine) *DbPooling {
	h._log = l
	return h
}

// PoolItem is Item in the pool
type PoolItem struct {
	sync.RWMutex
	conn   IConnection
	used   bool
	closed bool

	lastUsed time.Time

	AutoRelease time.Duration
	AutoClose   time.Duration

	_log *toolkit.LogEngine
	ID   int
}

func (h *PoolItem) Log() *toolkit.LogEngine {
	if h._log == nil {
		h._log = toolkit.NewLogEngine(true, false, "", "", "")
	}
	return h._log
}

func (h *PoolItem) SetLog(l *toolkit.LogEngine) *PoolItem {
	h._log = l
	return h
}

// NewDbPooling create new pooling with given size
func NewDbPooling(size int, fnNew func() (IConnection, error)) *DbPooling {
	dbp := new(DbPooling)
	dbp.size = size
	dbp.fnNew = fnNew
	dbp.Timeout = DefaultPoolingTimeout
	return dbp
}

// Get new connection. If all connection is being used and number of connection is less than
// pool capacity, new connection will be spin off. If capabity has been max out. It will waiting for
// any connection to be released before timeout reach
func (p *DbPooling) Get() (*PoolItem, error) {
	timeoutDuration := p.Timeout
	if int(p.AutoRelease) > 0 {
		timeoutDuration += p.AutoRelease
	}

	cpi := make(chan *PoolItem)
	cerr := make(chan error)

	//--- remove closed pi
	/*
		hasChanged := false
		bufferItems := []*PoolItem{}
		for _, pi := range p.GetItems() {
			if !pi.isClosed() {
				hasChanged = true
				bufferItems = append(bufferItems, pi)
			}
		}

		if hasChanged {
			p.Lock()
			p.items = bufferItems
			p.Unlock()
		}
	*/

	go func(p *DbPooling) {
		// check if there is an idle connection from pool. if it is, then use it.
		p.RLock()
		for _, pi := range p.items {
			if pi != nil && pi.IsFree() {
				p.RUnlock()
				pi.Use()
				cpi <- pi
				return
			}
		}
		p.RUnlock()

		// no idle connections are found from the pool.
		// then perform another check.
		// if the total created connection is still lower than pool max conn size, create new one.
		newPoolItem := func() bool {
			p.Lock()
			shouldCreateNewPoolItem := len(p.items) < p.size

			if !shouldCreateNewPoolItem {
				p.Unlock()
				return false
			}

			pi, err := p.newItem()
			if err != nil {
				p.Unlock()
				cerr <- err
				return false
			}

			// add the newly created connection into pool
			p.items = append(p.items, pi)
			//fmt.Println(len(p.items), "|", toolkit.JsonString(p.items))
			p.Unlock()

			// use newly created connection, then end the routine
			pi.retrieveDbPoolingInfo(p)
			pi.Use()
			cpi <- pi
			return true
		}()
		if newPoolItem {
			return
		}

		// block underneath will only be executed if the two criteria below are met:
		// 1. no idle connection is found from the pool
		// 2. cannot create new connection, because total created conns met allowed max conns

		// what will happen next, we'll wait until `p.Timeout`.
		// - if one connection is found idle and not closed before exceeding timeout, then use that one
		// - if timeout is exceeded, then return an error
		t0 := time.Now()
		for done := false; !done; {
			<-time.After(10 * time.Millisecond)

			for _, pi := range p.GetItems() {
				if pi != nil && pi.IsFree() && !pi.isClosed() {
					//toolkit.Printfn("Connection is available, will be used. Size: %d Count: %d", p.Size(), p.Count())
					pi.retrieveDbPoolingInfo(p)
					pi.Use()
					cpi <- pi
					done = true
					return
				}
			}

			if int(timeoutDuration) > 0 && time.Since(t0) > timeoutDuration {
				done = true
				cerr <- fmt.Errorf("get connection timeout exceeded %s, connection: %d, free: %d, pool size: %d",
					timeoutDuration.String(), p.Count(), p.FreeCount(), p.Size())
				return
			}
		}
	}(p)

	select {
	case pi := <-cpi:
		//toolkit.Printfn("Connection is used. Size: %d Count: %d", p.Size(), p.Count())
		return pi, nil
	case err := <-cerr:
		return nil, toolkit.Errorf("unable to get pool item. %s", err.Error())
	}
}

// GetItems return pool items within connection pooling
func (p *DbPooling) GetItems() []*PoolItem {
	p.RLock()
	items := p.items
	p.RUnlock()
	return items
}

// Count number of connection within connection pooling
func (p *DbPooling) Count() int {
	return len(p.GetItems())
}

// FreeCount number of item has been released
func (p *DbPooling) FreeCount() int {
	i := 0
	items := p.GetItems()
	for _, pi := range items {
		if pi != nil && pi.IsFree() && !pi.closed {
			i++
		}
	}
	return i
}

// ClosedCount number of item has been closed
func (p *DbPooling) ClosedCount() int {
	i := 0
	for _, pi := range p.GetItems() {
		if pi != nil && pi.closed {
			i++
		}
	}
	return i
}

// Size number of connection can be hold within the connection pooling
func (p *DbPooling) Size() int {
	return p.size
}

// Close all connection within connection pooling
func (p *DbPooling) Close() {
	p.Lock()
	for _, pi := range p.items {
		if pi != nil {
			pi.conn.Close()
		}
	}

	p.items = []*PoolItem{}
	p.Unlock()
}

func (p *DbPooling) newItem() (*PoolItem, error) {
	conn, err := p.fnNew()
	if err != nil {
		return nil, toolkit.Errorf("unable to open connection for DB pool. %s", err.Error())
	}

	pi := &PoolItem{conn: conn, used: false}
	pi.SetLog(p.Log())
	pi.retrieveDbPoolingInfo(p)
	//t0 := time.Now()
	hashClose := toolkit.ToInt(toolkit.Date2String(time.Now(), "HHmmss"), toolkit.RoundingAuto)*1000 + toolkit.RandInt(1000)
	pi.ID = hashClose
	/*
		pi.Log().Infof("Hub.Connection: Initiate: %d release: %s, close: %s. Connections count: %d",
			hashClose,
			pi.AutoRelease.String(),
			pi.AutoClose.String(),
			len(p.GetItems())+1)
	*/

	//-- auto release if it's enabled
	if pi.AutoRelease > 0 {
		go func(pi *PoolItem) {
			for {
				if pi == nil {
					return
				}

				<-time.After(100 * time.Millisecond)
				pi.RLock()
				diff := time.Since(pi.lastUsed)
				pi.RUnlock()
				if diff > pi.AutoRelease && !pi.IsFree() {
					pi.Release()
					/*
						pi.Log().Infof("Hub.Connection: Releasing %d after %s. Connections count: %d",
							hashClose, diff.String(), len(p.GetItems()))
					*/
					return
				}
			}
		}(pi)
	}

	//-- auto close if it's enabled
	if pi.AutoClose > 0 {
		go func(pi *PoolItem) {
			for {
				if pi == nil {
					return
				}

				<-time.After(100 * time.Millisecond)
				pi.RLock()
				diff := time.Since(pi.lastUsed)
				pi.RUnlock()
				if diff > pi.AutoClose && pi.IsFree() {
					p.Lock()
					pi.conn.Close()
					pi.closed = true

					items := p.items
					unclosedItems := make([]*PoolItem, len(items))
					unclosedCount := 0
					for _, it := range items {
						if it == nil {

						} else if !it.closed {
							unclosedItems = append(unclosedItems, it)
							unclosedCount++
						}
					}
					p.items = unclosedItems[:unclosedCount]

					p.Unlock()
					/*
						pi.Log().Infof("Hub.Connection: Closing %d after %s. Connections count: %d",
							hashClose, diff.String(), len(p.GetItems())-1)
					*/
					return
				}
			}
		}(pi)
	}

	return pi, nil
}

func (pi *PoolItem) retrieveDbPoolingInfo(p *DbPooling) {
	pi.AutoClose = p.AutoClose
	pi.AutoRelease = p.AutoRelease
}

func (pi *PoolItem) isClosed() bool {
	ret := false
	pi.RLock()
	ret = pi.closed
	pi.RUnlock()
	return ret
}

// Release PoolItem
func (pi *PoolItem) Release() {
	pi.Lock()
	pi.used = false
	pi.lastUsed = time.Now()
	pi.Unlock()
	//pi.Log().Infof("Hub.Connection: Releasing %d", pi.ID)
}

// IsFree check and return true if PoolItem is free
func (pi *PoolItem) IsFree() bool {
	free := false
	pi.RLock()
	free = !pi.used
	pi.RUnlock()
	return free
}

// Use mark that this PoolItem is used
func (pi *PoolItem) Use() {
	pi.Lock()
	pi.used = true
	pi.lastUsed = time.Now()
	pi.Unlock()
	//pi.Log().Infof("Hub.Connection: Re-Use %d", pi.ID)
}

// Connection return PoolItem connection
func (pi *PoolItem) Connection() IConnection {
	return pi.conn
}
