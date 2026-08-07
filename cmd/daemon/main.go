package main

import (
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"botmux-go/internal/config"
	"botmux-go/internal/daemon"
	_ "botmux-go/internal/adapter"
	"botmux-go/internal/protocol"
	"botmux-go/internal/worker"
)

const (
	EnvRole      = "BOTMUX_ROLE"
	EnvSessionID = "BOTMUX_SESSION_ID"
	EnvDaemonAddr = "BOTMUX_DAEMON_ADDR"
	EnvCliType   = "BOTMUX_CLI_TYPE"
	EnvCliPath   = "BOTMUX_CLI_PATH"
	EnvWorkingDir = "BOTMUX_WORKING_DIR"
	EnvStoreDir  = "BOTMUX_STORE_DIR"
)

func main() {
	if role := os.Getenv(EnvRole); role == "worker" {
		runWorker()
		return
	}
	runDaemon()
}

func runWorker() {
	sessionID := os.Getenv(EnvSessionID)
	if sessionID == "" {
		log.Fatalf("[worker] %s required", EnvSessionID)
	}
	daemonAddr := os.Getenv(EnvDaemonAddr)
	if daemonAddr == "" {
		daemonAddr = "127.0.0.1:17890"
	}
	cliType := os.Getenv(EnvCliType)
	if cliType == "" {
		cliType = "mock"
	}
	cliPath := os.Getenv(EnvCliPath)
	workingDir := os.Getenv(EnvWorkingDir)
	if workingDir == "" {
		home, _ := os.UserHomeDir()
		workingDir = home
	}
	storeDir := os.Getenv(EnvStoreDir)
	w := worker.New(worker.Options{
		SessionID:  sessionID,
		DaemonAddr: daemonAddr,
		CliType:    cliType,
		CliPath:    cliPath,
		WorkingDir: workingDir,
		StoreDir:   storeDir,
	})
	if err := w.Run(); err != nil {
		log.Fatalf("[worker] fatal: %v", err)
	}
}

func runDaemon() {
	cfgPath := flag.String("config", "", "path to bots.json config (optional)")
	cmd := flag.String("cmd", "", "send command to running daemon: new <session_id> [<bot_id>] | send <session_id> <msg>")
	listen := flag.String("listen", "", "override daemon listen addr (e.g. 127.0.0.1:17890)")
	flag.Parse()

	if *cmd != "" {
		execCommand(*listen, *cmd, flag.Args())
		return
	}

	cfg, err := config.Load(*cfgPath)
	if err != nil {
		log.Printf("[daemon] load config failed, using defaults: %v", err)
		cfg = config.DefaultConfig()
	}
	if *listen != "" {
		cfg.ListenAddr = *listen
	}
	d, err := daemon.New(cfg)
	if err != nil {
		log.Fatalf("[daemon] init: %v", err)
	}
	if err := d.Start(); err != nil {
		log.Fatalf("[daemon] start: %v", err)
	}

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)

	fmt.Printf("\n=== botmux-go daemon started ===\n")
	fmt.Printf("  listen   : %s\n", cfg.ListenAddr)
	fmt.Printf("  bots     : %d\n", len(cfg.Bots))
	for _, b := range cfg.Bots {
		fmt.Printf("    - %s (%s, cli=%s, backend=%s)\n", b.Name, b.BotID, b.CliType, b.BackendType)
	}
	fmt.Printf("  new sess : %s -cmd new <sid> [<bot_id>]\n", os.Args[0])
	fmt.Printf("  send msg : %s -cmd send <sid> \"hello world\"\n", os.Args[0])
	fmt.Printf("  shutdown : ctrl+c\n\n")

	go func() {
		<-sigs
		fmt.Println("\n[daemon] shutting down...")
		_ = d.Stop()
	}()

	d.Wait()
	fmt.Println("[daemon] stopped")
}

func daemonAddrOr(override string) string {
	if override != "" {
		return override
	}
	if v := os.Getenv(EnvDaemonAddr); v != "" {
		return v
	}
	return "127.0.0.1:17890"
}

func ensureDaemonReachable(addr string) {
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		fmt.Printf("[cli] cannot reach daemon at %s: %v\n", addr, err)
		fmt.Printf("[cli] please start daemon first: %s\n", os.Args[0])
		os.Exit(1)
	}
	conn.Close()
}

func execCommand(daemonOverride, sub string, rest []string) {
	addr := daemonAddrOr(daemonOverride)
	ensureDaemonReachable(addr)

	switch sub {
	case "new", "ns", "newsession":
		sid := ""
		botID := ""
		if len(rest) > 0 {
			sid = rest[0]
		}
		if len(rest) > 1 {
			botID = rest[1]
		}
		if sid == "" {
			sid = fmt.Sprintf("s-%d", time.Now().Unix())
		}
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			fmt.Printf("[cli] connect: %v\n", err)
			os.Exit(1)
		}
		defer conn.Close()
		m := protocol.NewMessage(protocol.MsgNewSession, sid, botID)
		fmt.Printf("[cli] -> %s new session %s (bot=%s)\n", addr, short(sid), botID)
		if _, err := m.WriteTo(conn); err != nil {
			fmt.Printf("[cli] write: %v\n", err)
			os.Exit(1)
		}
		br := protocol.NewMessageReader(conn)
		deadline := time.Now().Add(18 * time.Second)
		gotAck := false
		for time.Now().Before(deadline) {
			_ = conn.SetReadDeadline(time.Now().Add(800 * time.Millisecond))
			msg, err := br.Read()
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					continue
				}
				fmt.Printf("[cli] read: %v\n", err)
				return
			}
			_ = conn.SetReadDeadline(time.Time{})
			if !gotAck && msg.Type == protocol.MsgAck && msg.SessionID == sid {
				gotAck = true
				if strings.HasPrefix(msg.Payload, "err:") {
					fmt.Printf("[cli] new session error: %s\n", strings.TrimPrefix(msg.Payload, "err:"))
					os.Exit(1)
				}
				fmt.Printf("[cli] session %s accepted, waiting for worker...\n", short(sid))
				continue
			}
			if msg.Type == protocol.MsgReady && msg.SessionID == sid {
				fmt.Printf("[cli] session %s READY. Next step:\n  %s -cmd send %s \"hello botmux\"\n",
					short(sid), os.Args[0], sid)
				return
			}
		}
		fmt.Printf("[cli] timeout waiting for READY. Check daemon logs.\n")

	case "send", "s":
		if len(rest) < 1 {
			fmt.Printf("usage: %s -cmd send <session_id> <message>\n", os.Args[0])
			os.Exit(1)
		}
		sid := rest[0]
		message := strings.Join(rest[1:], " ")
		if message == "" {
			fmt.Printf("message is empty\n")
			os.Exit(1)
		}
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			fmt.Printf("[cli] connect daemon %s: %v\n", addr, err)
			os.Exit(1)
		}
		defer conn.Close()
		m := protocol.NewMessage(protocol.MsgUserInput, sid, message)
		fmt.Printf("[cli] -> session %s type=user_input payload=%q\n", short(sid), message)
		if _, err := m.WriteTo(conn); err != nil {
			fmt.Printf("[cli] write: %v\n", err)
			os.Exit(1)
		}
		br := protocol.NewMessageReader(conn)
		fmt.Printf("[output] waiting...\n")
		deadline := time.Now().Add(10 * time.Second)
		gotAny := false
		for time.Now().Before(deadline) {
			_ = conn.SetReadDeadline(time.Now().Add(800 * time.Millisecond))
			msg, err := br.Read()
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					continue
				}
				break
			}
			_ = conn.SetReadDeadline(time.Time{})
			if msg.Type == protocol.MsgError && (msg.SessionID == sid || msg.SessionID == "") {
				fmt.Printf("  !! error: %s\n", msg.Payload)
				os.Exit(1)
			}
			if msg.SessionID == sid && msg.Type == protocol.MsgOutput {
				fmt.Printf("  << %s\n", msg.Payload)
				gotAny = true
				break
			}
		}
		_ = conn.SetReadDeadline(time.Time{})
		if gotAny {
			fmt.Printf("[cli] done\n")
		} else {
			fmt.Printf("[cli] no output received within deadline. Is session %s running?\n", short(sid))
		}

	default:
		fmt.Printf("unknown -cmd %q. Supported: new, send\n", sub)
		os.Exit(2)
	}
}

func short(s string) string {
	if len(s) <= 8 {
		return s
	}
	return s[:8]
}
