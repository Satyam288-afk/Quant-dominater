package sandbox

import (
	"testing"
	"time"
)

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

func TestDockerNetworkPlanUsesHostReachableBridge(t *testing.T) {
	for _, spec := range []SandboxSpec{
		{NetworkEgress: false},
		{NetworkEgress: true},
	} {
		plan := dockerNetworkPlanFor(spec, "sandbox_123")
		if plan.isolated {
			t.Fatal("Docker mode should stay host-reachable on bridge; hard egress denial is the K8s path")
		}
		if plan.name != "bridge" {
			t.Fatalf("network name = %q, want bridge", plan.name)
		}
		if string(plan.mode) != "bridge" {
			t.Fatalf("network mode = %q, want bridge", plan.mode)
		}
	}
}

func TestDockerContainerStartTimeoutAllowsColdDocker(t *testing.T) {
	if dockerContainerStartTimeout < 2*time.Minute {
		t.Fatalf("dockerContainerStartTimeout = %s, want at least 2m", dockerContainerStartTimeout)
	}
	if dockerHostPortTimeout < time.Minute {
		t.Fatalf("dockerHostPortTimeout = %s, want at least 1m", dockerHostPortTimeout)
	}
	if dockerInspectTimeout > 2*time.Second {
		t.Fatalf("dockerInspectTimeout = %s, want at most 2s", dockerInspectTimeout)
	}
}
