package tree

import "testing"

// TestGlobalStopKeepsSystemRunning 验证全局入口清理单个 Actor 后仍可创建新 Actor。
func TestGlobalStopKeepsSystemRunning(t *testing.T) {
	if !isolatedGlobalTest(t) {
		return
	}
	for i := 0; i < 2; i++ {
		pid := SpawnOne(&shutdownActor{name: "global-entry"})
		if found, ok := Lookup("global-entry"); !ok || found != pid {
			t.Fatal("global name lookup failed")
		}
		if !TrySend(pid, "message") {
			t.Fatal("global send failed")
		}
		Stop(pid)
		if TrySend(pid, "late") {
			t.Fatal("stopped actor accepts messages")
		}
		if _, ok := Lookup("global-entry"); ok {
			t.Fatal("stopped name retained")
		}
	}
}
