package events

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	"github.com/nabutabu/herdr-scribe/internal/client"
)

func TestMain(m *testing.M) {
	slog.SetDefault(slog.New(slog.NewTextHandler(io.Discard, nil)))
	os.Exit(m.Run())
}

// compareSubscriptions compares the subscription payloads semantically,
// ignoring JSON key ordering differences between struct-marshalled params and
// received-then-remarshalled params.
func compareSubscriptions(got, want map[string]any) error {
	var gotSubs, wantSubs []Subscription
	gotRaw, err := json.Marshal(got["subscriptions"])
	if err != nil {
		return fmt.Errorf("marshalling received subscriptions: %w", err)
	}
	wantRaw, err := json.Marshal(want["subscriptions"])
	if err != nil {
		return fmt.Errorf("marshalling wanted subscriptions: %w", err)
	}
	if err := json.Unmarshal(gotRaw, &gotSubs); err != nil {
		return fmt.Errorf("unmarshalling received subscriptions: %w", err)
	}
	if err := json.Unmarshal(wantRaw, &wantSubs); err != nil {
		return fmt.Errorf("unmarshalling wanted subscriptions: %w", err)
	}
	if !reflect.DeepEqual(gotSubs, wantSubs) {
		return fmt.Errorf("subscriptions = %+v, want %+v", gotSubs, wantSubs)
	}
	return nil
}

func startStubServer(t *testing.T, handler func(*bufio.Reader, net.Conn) error) (string, <-chan error) {
	t.Helper()
	sockPath := filepath.Join(t.TempDir(), "herdr-events-test.sock")
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { ln.Close() })

	errCh := make(chan error, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()
		errCh <- handler(bufio.NewReader(conn), conn)
	}()

	return sockPath, errCh
}

func startEventsSubscriber(t *testing.T, params map[string]any) *Subscriber {
	t.Helper()
	sub, err := NewSubscriber(params)
	if err != nil {
		t.Fatalf("NewSubscriber: %v", err)
	}
	t.Cleanup(func() { sub.Close() })
	return sub
}

func setSocketPath(t *testing.T, path string) {
	t.Helper()
	t.Setenv("HERDR_SOCKET_PATH", path)
}

func readSubscribeFrame(r *bufio.Reader) (*client.Request, error) {
	line, err := r.ReadBytes('\n')
	if err != nil {
		return nil, fmt.Errorf("reading subscribe frame: %w", err)
	}
	var req client.Request
	if err := json.Unmarshal(line, &req); err != nil {
		return nil, fmt.Errorf("parsing subscribe frame %q: %w", string(line), err)
	}
	if req.ID != "herdr-scribe" || req.Method != "events.subscribe" {
		return nil, fmt.Errorf("unexpected subscribe frame: %+v", req)
	}
	return &req, nil
}

func TestSubscribeWritesSingleFrame(t *testing.T) {
	sock, errCh := startStubServer(t, func(r *bufio.Reader, conn net.Conn) error {
		req, err := readSubscribeFrame(r)
		if err != nil {
			return err
		}

		if err := compareSubscriptions(req.Params, BuildParams(nil)); err != nil {
			return err
		}

		// No writes may follow the subscribe frame on this connection: a read
		// that returns data (rather than EOF/deadline) is a violation.
		conn.SetReadDeadline(time.Now().Add(300 * time.Millisecond))
		if extra, err := r.ReadBytes('\n'); err == nil {
			return fmt.Errorf("unexpected client write after subscribe: %q", extra)
		}
		return nil
	})
	setSocketPath(t, sock)

	sub := startEventsSubscriber(t, BuildParams(nil))
	if err := sub.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("stub server: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stub server did not finish")
	}
}

func TestSubscribePaneScopedParams(t *testing.T) {
	sock, errCh := startStubServer(t, func(r *bufio.Reader, conn net.Conn) error {
		req, err := readSubscribeFrame(r)
		if err != nil {
			return err
		}

		if err := compareSubscriptions(req.Params, BuildParams([]string{"w1:p1", "w1:p2"})); err != nil {
			return err
		}
		return nil
	})
	setSocketPath(t, sock)

	startEventsSubscriber(t, BuildParams([]string{"w1:p1", "w1:p2"})).Close()

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("stub server: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stub server did not finish")
	}
}

func TestSubscribeReceivesEventsVerbatim(t *testing.T) {
	events := []string{
		`{"type":"workspace.created","workspace_id":"w1","number":1,"label":"~"}`,
		`{"type":"pane.created","pane_id":"w1:p1","workspace_id":"w1","tab_id":"w1:t1"}`,
		`{"type":"pane.agent_detected","agent":"codex","pane_id":"w1:p1","workspace_id":"w1"}`,
		`{"type":"pane.agent_status_changed","agent":"codex","agent_status":"working","pane_id":"w1:p1","workspace_id":"w1"}`,
	}

	sock, _ := startStubServer(t, func(r *bufio.Reader, conn net.Conn) error {
		if _, err := readSubscribeFrame(r); err != nil {
			return err
		}
		for _, e := range events {
			if _, err := fmt.Fprintf(conn, "%s\n", e); err != nil {
				return err
			}
		}
		return nil
	})
	setSocketPath(t, sock)

	sub := startEventsSubscriber(t, BuildParams(nil))
	for i, want := range events {
		select {
		case got, ok := <-sub.Events():
			if !ok {
				t.Fatalf("events channel closed before event %d", i)
			}
			if string(got) != want {
				t.Errorf("event %d = %s, want %s", i, got, want)
			}
		case <-time.After(2 * time.Second):
			t.Fatalf("timed out waiting for event %d", i)
		}
	}
}

func TestSubscribeDropsAck(t *testing.T) {
	const ev = `{"type":"pane.created","pane_id":"w1:p1","workspace_id":"w1"}`

	sock, _ := startStubServer(t, func(r *bufio.Reader, conn net.Conn) error {
		if _, err := readSubscribeFrame(r); err != nil {
			return err
		}
		for _, frame := range []string{
			`{"id":"herdr-scribe","result":{"ok":true}}`,
			ev,
		} {
			if _, err := fmt.Fprintf(conn, "%s\n", frame); err != nil {
				return err
			}
		}
		return nil
	})
	setSocketPath(t, sock)

	sub := startEventsSubscriber(t, BuildParams(nil))
	select {
	case got, ok := <-sub.Events():
		if !ok {
			t.Fatal("events channel closed before the real event")
		}
		if string(got) != ev {
			t.Errorf("event = %s, want %s", got, ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event after ack")
	}

	// The ack must not be delivered; nothing but the ack may follow.
	select {
	case _, ok := <-sub.Events():
		if ok {
			t.Error("unexpected extra event after ack was dropped")
		}
	case <-time.After(150 * time.Millisecond):
	}
}

func TestSubscribeSkipsMalformed(t *testing.T) {
	const ev = `{"type":"pane.closed","pane_id":"w1:p1","workspace_id":"w1"}`

	sock, _ := startStubServer(t, func(r *bufio.Reader, conn net.Conn) error {
		if _, err := readSubscribeFrame(r); err != nil {
			return err
		}
		for _, frame := range []string{
			"this is not json at all",
			`{"unrelated":true}`,
			ev,
		} {
			if _, err := fmt.Fprintf(conn, "%s\n", frame); err != nil {
				return err
			}
		}
		return nil
	})
	setSocketPath(t, sock)

	sub := startEventsSubscriber(t, BuildParams(nil))
	select {
	case got, ok := <-sub.Events():
		if !ok {
			t.Fatal("events channel closed before the real event")
		}
		if string(got) != ev {
			t.Errorf("event = %s, want %s", got, ev)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for event after malformed frames")
	}
}

func TestSubscribeSurfacesEOF(t *testing.T) {
	sock, _ := startStubServer(t, func(r *bufio.Reader, conn net.Conn) error {
		if _, err := readSubscribeFrame(r); err != nil {
			return err
		}
		// Close the connection without pushing anything.
		return nil
	})
	setSocketPath(t, sock)

	sub := startEventsSubscriber(t, BuildParams(nil))
	select {
	case err := <-sub.Err():
		if err == nil {
			t.Fatal("expected a stream error after EOF")
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for stream error")
	}

	if _, ok := <-sub.Events(); ok {
		t.Error("events channel still open after stream error")
	}
}

func TestSubscribeCloseIdempotent(t *testing.T) {
	sock, errCh := startStubServer(t, func(r *bufio.Reader, conn net.Conn) error {
		if _, err := readSubscribeFrame(r); err != nil {
			return err
		}
		// Keep the connection open until the client closes it.
		buf := make([]byte, 1)
		for {
			if _, err := conn.Read(buf); err != nil {
				return nil
			}
		}
	})
	setSocketPath(t, sock)

	sub := startEventsSubscriber(t, BuildParams(nil))
	if err := sub.Close(); err != nil {
		t.Fatalf("first Close: %v", err)
	}
	if err := sub.Close(); err != nil {
		t.Fatalf("second Close: %v", err)
	}

	select {
	case <-sub.done:
	case <-time.After(2 * time.Second):
		t.Fatal("reader goroutine did not exit after Close")
	}

	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("stub server: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("stub server did not finish")
	}
}

func TestClassify(t *testing.T) {
	event := json.RawMessage(`{"type":"pane.created","pane_id":"w1:p1"}`)
	if got := classify(event); got != frameEvent {
		t.Errorf("classify(event) = %v, want frameEvent", got)
	}

	ack := json.RawMessage(`{"id":"herdr-scribe","result":{"ok":true}}`)
	if got := classify(ack); got != frameAck {
		t.Errorf("classify(ack) = %v, want frameAck", got)
	}

	errResp := json.RawMessage(`{"id":"herdr-scribe","error":{"code":"MethodNotFound","message":"nope"}}`)
	if got := classify(errResp); got != frameAck {
		t.Errorf("classify(error response) = %v, want frameAck", got)
	}

	unknown := json.RawMessage(`{"unrelated":1}`)
	if got := classify(unknown); got != frameUnknown {
		t.Errorf("classify(unknown) = %v, want frameUnknown", got)
	}

	garbage := json.RawMessage(`not json`)
	if got := classify(garbage); got != frameUnknown {
		t.Errorf("classify(garbage) = %v, want frameUnknown", got)
	}
}
