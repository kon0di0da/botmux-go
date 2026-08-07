package daemon

import (
	"log"
	"sync"
	"time"
)

const (
	monitorInterval     = 1 * time.Second
	workerAliveTimeout  = 30 * time.Second
	maxSpawnRetries     = 5
	monitorStartDelay   = 3 * time.Second
	maxConcurrentSpawns = 5
)

type spawnFailure struct {
	count     int
	lastRetry time.Time
}

func (d *Daemon) startSessionMonitor() {
	d.monitorMu.Lock()
	if d.monitorStop != nil {
		d.monitorMu.Unlock()
		return
	}
	d.monitorStop = make(chan struct{})
	d.spawnFailures = make(map[string]*spawnFailure)
	d.spawnSem = make(chan struct{}, maxConcurrentSpawns)
	d.monitorMu.Unlock()

	d.wg.Add(1)
	go func() {
		defer d.wg.Done()
		time.Sleep(monitorStartDelay)
		ticker := time.NewTicker(monitorInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				d.reconcileSessions()
			case <-d.monitorStop:
				return
			case <-d.ctx.Done():
				return
			}
		}
	}()
	log.Printf("[daemon-monitor] started (interval=%v, max_concurrent_spawns=%d, start_delay=%v)",
		monitorInterval, maxConcurrentSpawns, monitorStartDelay)
}

func (d *Daemon) stopSessionMonitor() {
	d.monitorMu.Lock()
	ch := d.monitorStop
	d.monitorStop = nil
	d.monitorMu.Unlock()
	if ch != nil {
		close(ch)
	}
}

func (d *Daemon) reconcileSessions() {
	d.sessionsMu.RLock()
	metas := make([]*SessionMeta, 0, len(d.sessions))
	for _, m := range d.sessions {
		metas = append(metas, m)
	}
	d.sessionsMu.RUnlock()

	var wg sync.WaitGroup
	for _, meta := range metas {
		if meta.Closed {
			continue
		}
		d.workersMu.RLock()
		h, hasH := d.workers[meta.SessionID]
		d.workersMu.RUnlock()

		needsSpawn := false
		if !hasH {
			needsSpawn = true
		} else if !h.IsAlive() && meta.Status != StatusSpawning {
			needsSpawn = true
		}
		if needsSpawn {
			d.monitorMu.Lock()
			sf, ok := d.spawnFailures[meta.SessionID]
			if !ok {
				sf = &spawnFailure{}
				d.spawnFailures[meta.SessionID] = sf
			}
			if sf.count >= maxSpawnRetries && time.Since(sf.lastRetry) < 1*time.Minute {
				d.monitorMu.Unlock()
				if sf.count == maxSpawnRetries {
					log.Printf("[daemon-monitor] session %s: spawn retries exhausted (%d), will retry later", safeShort(meta.SessionID), maxSpawnRetries)
					sf.count++
				}
				continue
			}
			sf.lastRetry = time.Now()
			d.monitorMu.Unlock()

			wg.Add(1)
			go func(m *SessionMeta, failures *spawnFailure) {
				defer wg.Done()
				select {
				case d.spawnSem <- struct{}{}:
				case <-d.ctx.Done():
					return
				}
				defer func() { <-d.spawnSem }()

				err := d.spawnWorkerForSession(m)
				if err != nil {
					d.monitorMu.Lock()
					failures.count++
					d.monitorMu.Unlock()
					log.Printf("[daemon-monitor] session %s: auto-spawn failed (err=%v, fail_count=%d)",
						safeShort(m.SessionID), err, failures.count)
				} else {
					d.monitorMu.Lock()
					failures.count = 0
					d.monitorMu.Unlock()
				}
			}(meta, sf)
		}
	}
	wg.Wait()
}
