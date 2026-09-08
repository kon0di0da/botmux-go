package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	_ "botmux-go/internal/adapter"
	"botmux-go/internal/config"
	"botmux-go/internal/daemon"
	"botmux-go/internal/protocol"
	"botmux-go/internal/worker"
)

const (
	EnvRole            = "BOTMUX_ROLE"
	EnvSessionID       = "BOTMUX_SESSION_ID"
	EnvDaemonAddr      = "BOTMUX_DAEMON_ADDR"
	EnvCliType         = "BOTMUX_CLI_TYPE"
	EnvCliPath         = "BOTMUX_CLI_PATH"
	EnvModel           = "BOTMUX_MODEL"
	EnvCodexProfile    = "BOTMUX_CODEX_PROFILE"
	EnvResumeSessionID = "BOTMUX_RESUME_SESSION_ID"
	EnvWorkingDir      = "BOTMUX_WORKING_DIR"
	EnvStoreDir        = "BOTMUX_STORE_DIR"
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
	model := os.Getenv(EnvModel)
	codexProfile := os.Getenv(EnvCodexProfile)
	resumeSessionID := os.Getenv(EnvResumeSessionID)
	workingDir := os.Getenv(EnvWorkingDir)
	if workingDir == "" {
		home, _ := os.UserHomeDir()
		workingDir = home
	}
	storeDir := os.Getenv(EnvStoreDir)
	w := worker.New(worker.Options{
		SessionID:       sessionID,
		DaemonAddr:      daemonAddr,
		CliType:         cliType,
		CliPath:         cliPath,
		Model:           model,
		CodexProfile:    codexProfile,
		ResumeSessionID: resumeSessionID,
		WorkingDir:      workingDir,
		StoreDir:        storeDir,
	})
	if err := w.Run(); err != nil {
		log.Fatalf("[worker] fatal: %v", err)
	}
}

func runDaemon() {
	cfgPath := flag.String("config", "", "path to bots.json config (optional)")
	cmd := flag.String("cmd", "", "send command to running daemon: new <session_id> [<bot_id>] [<codex_profile>] | send <session_id> <msg>")
	listen := flag.String("listen", "", "override daemon listen addr (e.g. 127.0.0.1:17890)")
	dashboard := flag.String("dashboard", "", "override dashboard listen addr (e.g. 127.0.0.1:17891)")
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
	if *dashboard != "" {
		cfg.DashboardAddr = *dashboard
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
	fmt.Printf("  listen    : %s\n", cfg.ListenAddr)
	fmt.Printf("  dashboard : %s\n", cfg.DashboardAddr)
	fmt.Printf("  bots      : %d\n", len(cfg.Bots))
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
		codexProfile := ""
		if len(rest) > 0 {
			sid = rest[0]
		}
		if len(rest) > 1 {
			botID = rest[1]
		}
		if len(rest) > 2 {
			codexProfile = rest[2]
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
		payload, err := json.Marshal(struct {
			BotID        string `json:"bot_id,omitempty"`
			CodexProfile string `json:"codex_profile,omitempty"`
		}{
			BotID:        botID,
			CodexProfile: codexProfile,
		})
		if err != nil {
			fmt.Printf("[cli] encode new session request: %v\n", err)
			os.Exit(1)
		}
		m := protocol.NewMessage(protocol.MsgNewSession, sid, string(payload))
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
		if err := execSendCommand(addr, sid, message, os.Stdout); err != nil {
			fmt.Printf("[cli] send: %v\n", err)
			os.Exit(1)
		}

	case "list", "ls", "sessions":
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			fmt.Printf("[cli] connect daemon %s: %v\n", addr, err)
			os.Exit(1)
		}
		defer conn.Close()
		_, _ = protocol.NewMessage(protocol.MsgListSessions, "", "").WriteTo(conn)
		br := protocol.NewMessageReader(conn)
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			_ = conn.SetReadDeadline(time.Now().Add(800 * time.Millisecond))
			msg, err := br.Read()
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					continue
				}
				break
			}
			if msg.Type == protocol.MsgError {
				fmt.Printf("  !! error: %s\n", msg.Payload)
				os.Exit(1)
			}
			if msg.Type == protocol.MsgListSessionsRsp {
				var entries []struct {
					SessionID  string   `json:"session_id"`
					BotID      string   `json:"bot_id"`
					CliType    string   `json:"cli_type"`
					Status     string   `json:"status"`
					Pid        int      `json:"pid"`
					LastActive string   `json:"last_active"`
					Outputs    []string `json:"outputs"`
				}
				if err := json.Unmarshal([]byte(msg.Payload), &entries); err != nil {
					fmt.Printf("[cli] parse list response failed: %v\n", err)
					os.Exit(1)
				}
				fmt.Printf("%-24s %-18s %-10s %-12s %-8s %-22s %s\n",
					"SESSION_ID", "BOT", "CLI", "STATUS", "PID", "LAST_ACTIVE", "LAST_OUTPUT")
				for _, e := range entries {
					pidStr := "-"
					if e.Pid > 0 {
						pidStr = fmt.Sprintf("%d", e.Pid)
					}
					lastOut := ""
					if n := len(e.Outputs); n > 0 {
						lastOut = e.Outputs[n-1]
						if len(lastOut) > 50 {
							lastOut = lastOut[:50] + "..."
						}
					}
					sid := e.SessionID
					if len(sid) > 22 {
						sid = sid[:22] + ".."
					}
					fmt.Printf("%-24s %-18s %-10s %-12s %-8s %-22s %s\n",
						sid, e.BotID, e.CliType, e.Status, pidStr, e.LastActive, lastOut)
				}
				fmt.Printf("\n[cli] %d session(s)\n", len(entries))
				return
			}
		}
		fmt.Printf("[cli] timeout waiting for list response\n")
		os.Exit(1)

	case "history", "hs":
		if len(rest) < 1 {
			fmt.Printf("usage: %s -cmd history <session_id>\n", os.Args[0])
			os.Exit(1)
		}
		sid := rest[0]
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			fmt.Printf("[cli] connect daemon %s: %v\n", addr, err)
			os.Exit(1)
		}
		defer conn.Close()
		_, _ = protocol.NewMessage(protocol.MsgHistory, sid, "").WriteTo(conn)
		br := protocol.NewMessageReader(conn)
		deadline := time.Now().Add(8 * time.Second)
		for time.Now().Before(deadline) {
			_ = conn.SetReadDeadline(time.Now().Add(800 * time.Millisecond))
			msg, err := br.Read()
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					continue
				}
				break
			}
			if msg.Type == protocol.MsgError && (msg.SessionID == sid || msg.SessionID == "") {
				fmt.Printf("  !! error: %s\n", msg.Payload)
				os.Exit(1)
			}
			if msg.Type == protocol.MsgHistoryRsp && msg.SessionID == sid {
				var outs []string
				if err := json.Unmarshal([]byte(msg.Payload), &outs); err != nil {
					fmt.Printf("[cli] parse history response failed: %v\n", err)
					os.Exit(1)
				}
				if len(outs) == 0 {
					fmt.Printf("[cli] session %s has no output history\n", short(sid))
					return
				}
				width := len(fmt.Sprintf("%d", len(outs)))
				for i, line := range outs {
					fmt.Printf("  [%*d] %s\n", width, i+1, line)
				}
				fmt.Printf("[cli] %d line(s)\n", len(outs))
				return
			}
		}
		fmt.Printf("[cli] timeout waiting for history response\n")
		os.Exit(1)

	case "close", "rm", "delete":
		if len(rest) < 1 {
			fmt.Printf("usage: %s -cmd close <session_id> [<reason>]\n", os.Args[0])
			os.Exit(1)
		}
		sid := rest[0]
		reason := ""
		if len(rest) > 1 {
			reason = strings.Join(rest[1:], " ")
		}
		conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
		if err != nil {
			fmt.Printf("[cli] connect daemon %s: %v\n", addr, err)
			os.Exit(1)
		}
		defer conn.Close()
		_, _ = protocol.NewMessage(protocol.MsgCloseSession, sid, reason).WriteTo(conn)
		br := protocol.NewMessageReader(conn)
		deadline := time.Now().Add(10 * time.Second)
		for time.Now().Before(deadline) {
			_ = conn.SetReadDeadline(time.Now().Add(800 * time.Millisecond))
			msg, err := br.Read()
			if err != nil {
				if ne, ok := err.(net.Error); ok && ne.Timeout() {
					continue
				}
				break
			}
			if msg.Type == protocol.MsgError && (msg.SessionID == sid || msg.SessionID == "") {
				fmt.Printf("  !! error: %s\n", msg.Payload)
				os.Exit(1)
			}
			if msg.Type == protocol.MsgCloseSessionAck && msg.SessionID == sid {
				fmt.Printf("[cli] session %s closed (reason=%q)\n", short(sid), reason)
				return
			}
		}
		fmt.Printf("[cli] timeout waiting for close ack\n")
		os.Exit(1)

	default:
		fmt.Printf("unknown -cmd %q. Supported: new, send, list, history, close\n", sub)
		fmt.Printf("\nUsage:\n")
		fmt.Printf("  new <session_id> [<bot_id>]     Create a new session\n")
		fmt.Printf("  send <session_id> <msg>         Send message to session\n")
		fmt.Printf("  list                            List all sessions with status\n")
		fmt.Printf("  history <session_id>            Show session output history\n")
		fmt.Printf("  close <session_id> [<reason>]   Close a session (stop auto-recover)\n")
		os.Exit(2)
	}
}

func execSendCommand(addr, sid, message string, out io.Writer) error {
	conn, err := net.DialTimeout("tcp", addr, 3*time.Second)
	if err != nil {
		return fmt.Errorf("connect daemon %s: %w", addr, err)
	}
	defer conn.Close()

	if _, err := fmt.Fprintf(out, "[cli] -> session %s type=user_input payload=%q\n", short(sid), message); err != nil {
		return err
	}
	if _, err := protocol.NewMessage(protocol.MsgUserInput, sid, message).WriteTo(conn); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	if _, err := fmt.Fprintln(out, "[output] waiting..."); err != nil {
		return err
	}

	reader := protocol.NewMessageReader(conn)
	deadline := time.Now().Add(120 * time.Second)
	for time.Now().Before(deadline) {
		_ = conn.SetReadDeadline(time.Now().Add(800 * time.Millisecond))
		msg, err := reader.Read()
		if err != nil {
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return fmt.Errorf("read: %w", err)
		}
		_ = conn.SetReadDeadline(time.Time{})
		if msg.SessionID != sid && msg.SessionID != "" {
			continue
		}
		switch msg.Type {
		case protocol.MsgError:
			return fmt.Errorf("%s", msg.Payload)
		case protocol.MsgOutput:
			if _, err := fmt.Fprintf(out, "  << %s\n", msg.Payload); err != nil {
				return err
			}
		case protocol.MsgTurnCompleted:
			var terminal protocol.TurnTerminal
			if err := json.Unmarshal([]byte(msg.Payload), &terminal); err != nil {
				return fmt.Errorf("decode turn terminal: %w", err)
			}
			if terminal.Status != protocol.TurnCompleted {
				return fmt.Errorf("%s: %s", terminal.ErrorCode, terminal.ErrorDetail)
			}
			_, err := fmt.Fprintln(out, "[cli] done")
			return err
		}
	}
	return fmt.Errorf("timeout waiting for turn completion")
}

func short(s string) string {
	if len(s) <= 8 {
		return s
	}
	return s[:8]
}
