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
	spawnRetryCooldown  = 1 * time.Minute
	monitorStartDelay   = 3 * time.Second
	maxConcurrentSpawns = 5
)

type spawnFailure struct {
	count           int
	lastRetry       time.Time
	exhaustedLogged bool
}

func (d *Daemon) recordSpawnFailure(sessionID string) int {
	d.monitorMu.Lock()
	defer d.monitorMu.Unlock()
	if d.spawnFailures == nil {
		d.spawnFailures = make(map[string]*spawnFailure)
	}
	sf := d.spawnFailures[sessionID]
	if sf == nil {
		sf = &spawnFailure{}
		d.spawnFailures[sessionID] = sf
	}
	sf.count++
	sf.lastRetry = time.Now()
	sf.exhaustedLogged = false
	return sf.count
}

func (d *Daemon) clearSpawnFailure(sessionID string) {
	d.monitorMu.Lock()
	delete(d.spawnFailures, sessionID)
	d.monitorMu.Unlock()
}

func (d *Daemon) isCurrentWorker(handle *WorkerHandle) bool {
	d.workersMu.RLock()
	current := d.workers[handle.SessionID]
	d.workersMu.RUnlock()
	return current == handle
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
			now := time.Now()
			d.monitorMu.Lock()
			sf, ok := d.spawnFailures[meta.SessionID]
			if !ok {
				sf = &spawnFailure{}
				d.spawnFailures[meta.SessionID] = sf
			}
			if sf.count >= maxSpawnRetries && now.Sub(sf.lastRetry) < spawnRetryCooldown {
				shouldLog := !sf.exhaustedLogged
				sf.exhaustedLogged = true
				count := sf.count
				d.monitorMu.Unlock()
				if shouldLog {
					log.Printf("[daemon-monitor] session %s: spawn retries exhausted (%d), cooling down for %v",
						safeShort(meta.SessionID), count, spawnRetryCooldown)
				}
				continue
			}
			sf.lastRetry = now
			sf.exhaustedLogged = false
			d.monitorMu.Unlock()

			wg.Add(1)
			go func(m *SessionMeta) {
				defer wg.Done()
				select {
				case d.spawnSem <- struct{}{}:
				case <-d.ctx.Done():
					return
				}
				defer func() { <-d.spawnSem }()

				err := d.spawnWorkerForSession(m)
				if err != nil {
					failCount := d.recordSpawnFailure(m.SessionID)
					log.Printf("[daemon-monitor] session %s: auto-spawn failed (err=%v, fail_count=%d)",
						safeShort(m.SessionID), err, failCount)
				}
			}(meta)
		}
	}
	wg.Wait()
}
