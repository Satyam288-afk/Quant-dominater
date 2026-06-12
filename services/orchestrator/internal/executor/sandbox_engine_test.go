package executor

import "testing"

func TestSandboxEngineUsesRunContextForTimeouts(t *testing.T) {
	engine := NewSandboxEngine("http://127.0.0.1:9200")
	if engine.client.Timeout != 0 {
		t.Fatalf("sandbox engine client timeout = %s, want no fixed timeout", engine.client.Timeout)
	}
}
