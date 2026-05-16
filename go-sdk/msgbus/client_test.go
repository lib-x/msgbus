package msgbus

import (
	"testing"
	"time"
)

func TestDefaultSubscribeOptions(t *testing.T) {
	origin := &NodeID{
		TenantId: "tenant",
		BlName:   "default",
		DeviceId: "device",
	}
	client := &Client{defaultOrigin: origin}

	opts := client.defaultSubscribeOptions(SubscribeOptions{})

	if opts.Origin != origin {
		t.Fatalf("expected default origin to be applied")
	}
	if opts.FromHeadID != 1 {
		t.Fatalf("expected from head 1, got %d", opts.FromHeadID)
	}
	if opts.ReplayLimit != DefaultReplayLimit {
		t.Fatalf("expected default replay limit, got %d", opts.ReplayLimit)
	}
	if opts.ReconnectDelay != 250*time.Millisecond {
		t.Fatalf("expected default reconnect delay, got %s", opts.ReconnectDelay)
	}
}

func TestDialRejectsNilContext(t *testing.T) {
	client, err := Dial(nil, "127.0.0.1:50051")
	if err == nil {
		_ = client.Close()
		t.Fatal("expected nil context error")
	}
}
