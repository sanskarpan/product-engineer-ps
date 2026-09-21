// Command server runs the durable reminders service.
//
// Env: PORT (8080), DB_PATH (./reminders.db), WORKERS (4),
// MAX_ATTEMPTS (5), BASE_DELAY_MS (500), MAX_DELAY_MS (30000),
// CLOCK_MODE (system|manual), CLOCK_START (RFC3339, manual mode),
// NOTIFY_MODE (ok|fail-first|always-temp|always-perm|lost-ack), NOTIFY_FAIL_FIRST (2).
package main

import (
	"context"
	"fmt"
	"log"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/sanskarpan/product-engineer-ps/solution/internal/api"
	"github.com/sanskarpan/product-engineer-ps/solution/internal/clock"
	"github.com/sanskarpan/product-engineer-ps/solution/internal/notify"
	"github.com/sanskarpan/product-engineer-ps/solution/internal/sched"
	"github.com/sanskarpan/product-engineer-ps/solution/internal/store"
)

func getenv(k, d string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return d
}

func getenvInt(k string, d int) int {
	if v := os.Getenv(k); v == "" {
		return d
	} else if n, err := strconv.Atoi(v); err != nil {
		log.Fatalf("invalid %s=%q: must be integer", k, v)
		return d
	} else {
		return n
	}
}

func main() {
	policy := sched.Policy{
		MaxAttempts: getenvInt("MAX_ATTEMPTS", 5),
		BaseDelayMs: int64(getenvInt("BASE_DELAY_MS", 500)),
		MaxDelayMs:  int64(getenvInt("MAX_DELAY_MS", 30000)),
	}
	if err := policy.Validate(); err != nil {
		log.Fatal(err)
	}
	var clk clock.Clock = clock.SystemClock{}
	var manual *clock.ManualClock
	if getenv("CLOCK_MODE", "system") == "manual" {
		start := getenv("CLOCK_START", "2026-09-20T00:00:00Z")
		t, err := time.Parse(time.RFC3339, start)
		if err != nil {
			log.Fatalf("bad CLOCK_START: %v", err)
		}
		manual = clock.NewManual(t)
		clk = manual
	}
	notifier := notify.NewFake(
		getenv("NOTIFY_MODE", notify.ModeOK),
		getenvInt("NOTIFY_FAIL_FIRST", 2),
	)
	s, err := store.Open(getenv("DB_PATH", "./reminders.db"), clk)
	if err != nil {
		log.Fatalf("open store: %v", err)
	}
	defer s.Close()

	sch := sched.New(s, notifier, clk, policy, slog.New(slog.NewTextHandler(os.Stderr, nil)))
	sch.Start(getenvInt("WORKERS", 4))
	defer sch.Stop()

	srv := api.New(s, sch, notifier, notifier, clk, manual)
	port := getenv("PORT", "8080")
	httpSrv := &http.Server{Addr: ":" + port, Handler: srv.Handler()}
	go func() {
		mode := "system"
		if manual != nil {
			mode = "manual@" + clk.Now().UTC().Format(time.RFC3339)
		}
		fmt.Printf("reminders on :%s clock=%s maxAttempts=%d baseDelayMs=%d\n",
			port, mode, policy.MaxAttempts, policy.BaseDelayMs)
		if err := httpSrv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("listen: %v", err)
		}
	}()

	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	fmt.Println("\nshutting down: draining HTTP, then scheduler, then store...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := httpSrv.Shutdown(ctx); err != nil {
		log.Printf("http shutdown: %v", err)
	}
}
