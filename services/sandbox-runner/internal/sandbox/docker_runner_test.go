package sandbox

import "testing"

func TestSanitizeDockerTag(t *testing.T) {
	got := sanitizeDockerTag("Team/Sub_01!")
	if got != "team-sub_01" {
		t.Fatalf("sanitizeDockerTag() = %q", got)
	}
}

func TestNormalizeDockerResources(t *testing.T) {
	if got := normalizeDockerMemory("512Mi"); got != "512m" {
		t.Fatalf("normalizeDockerMemory() = %q", got)
	}
	if got := normalizeDockerCPUs("500m"); got != "0.5" {
		t.Fatalf("normalizeDockerCPUs() = %q", got)
	}
}

func TestDockerNetworkPlanHonorsEgressFlag(t *testing.T) {
	isolated := dockerNetworkPlanFor(SandboxSpec{NetworkEgress: false}, "sandbox_123")
	if !isolated.isolated {
		t.Fatal("network_egress=false should create an isolated network")
	}
	if isolated.name != "iicpc-sandbox_123" {
		t.Fatalf("isolated network name = %q", isolated.name)
	}
	if string(isolated.mode) != isolated.name {
		t.Fatalf("isolated network mode = %q, want %q", isolated.mode, isolated.name)
	}

	bridge := dockerNetworkPlanFor(SandboxSpec{NetworkEgress: true}, "sandbox_123")
	if bridge.isolated {
		t.Fatal("network_egress=true should use the default bridge network")
	}
	if string(bridge.mode) != "bridge" {
		t.Fatalf("bridge network mode = %q", bridge.mode)
	}
}
