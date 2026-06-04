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
