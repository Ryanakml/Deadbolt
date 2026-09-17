package cli

import (
	"net"
	"testing"
)

// TestHostedCallbackListenerUsesPinnedLoopbackPort proves hosted browser
// login binds the exact loopback callback registered with the identity
// provider (http://127.0.0.1:8765/callback) instead of a random port.
func TestHostedCallbackListenerUsesPinnedLoopbackPort(t *testing.T) {
	listener, redirectURI, err := hostedCallbackListener()
	if err != nil {
		t.Fatalf("hostedCallbackListener failed: %v", err)
	}
	defer listener.Close()

	if expected := "http://127.0.0.1:8765/callback"; redirectURI != expected {
		t.Fatalf("expected redirect URI %q, got %q", expected, redirectURI)
	}
	addr, ok := listener.Addr().(*net.TCPAddr)
	if !ok || addr.IP.String() != "127.0.0.1" || addr.Port != hostedLoopbackCallbackPort {
		t.Fatalf("expected loopback listener on 127.0.0.1:%d, got %v", hostedLoopbackCallbackPort, listener.Addr())
	}

	// A second concurrent login must fail instead of silently falling back to
	// a random unregistered port.
	second, _, err := hostedCallbackListener()
	if err == nil {
		second.Close()
		t.Fatal("expected second hosted callback listener to fail while the fixed port is occupied")
	}
}
