package notify_test

import (
	"context"
	"path/filepath"
	"testing"

	"github.com/sanskarpan/product-engineer-ps/solution/internal/notify"
)

// The durable log closes the crash-between-send-and-record window: a new
// Fake on the same path must acknowledge the key without renotifying.
func TestPersistedDedupeAcrossRestart(t *testing.T) {
	path := filepath.Join(t.TempDir(), "deliveries.log")
	f1, err := notify.NewFakePersisted(path, notify.ModeOK, 0)
	if err != nil {
		t.Fatal(err)
	}
	if ok, _, _ := f1.Send(context.Background(), "r1:v1", "hi"); !ok {
		t.Fatal("first send must deliver")
	}
	if err := f1.Close(); err != nil {
		t.Fatal(err)
	}
	// Fresh process generation, same log file.
	f2, err := notify.NewFakePersisted(path, notify.ModeOK, 0)
	if err != nil {
		t.Fatal(err)
	}
	defer f2.Close()
	if ok, _, _ := f2.Send(context.Background(), "r1:v1", "hi"); !ok {
		t.Fatal("redelivery must be acknowledged")
	}
	if f2.LogicalCount() != 1 {
		t.Fatalf("restart redelivery notified again: logical=%d", f2.LogicalCount())
	}
	if ok, _, _ := f2.Send(context.Background(), "r2:v1", "hi"); !ok {
		t.Fatal("new key must deliver")
	}
	if f2.LogicalCount() != 2 {
		t.Fatalf("logical=%d want 2", f2.LogicalCount())
	}
}

func TestInMemoryModes(t *testing.T) {
	f := notify.NewFake(notify.ModeFailFirst, 1)
	if ok, kind, _ := f.Send(context.Background(), "a:v1", "x"); ok || kind != notify.Temporary {
		t.Fatalf("first must fail temporary: %v %s", ok, kind)
	}
	if ok, _, _ := f.Send(context.Background(), "a:v1", "x"); !ok {
		t.Fatal("retry must reconcile via the same key")
	}
	if f.LogicalCount() != 1 {
		t.Fatalf("logical=%d want 1", f.LogicalCount())
	}
}
