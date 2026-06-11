package executor

import (
	"strings"
	"testing"

	"orchestrator/internal/model"
)

func TestValidateBenchmarkLoadRejectsNoOrders(t *testing.T) {
	err := validateBenchmarkLoad(&model.Metrics{ConnectErrors: 10})
	if err == nil {
		t.Fatal("expected no-order benchmark to be rejected")
	}
	if !strings.Contains(err.Error(), "no orders") {
		t.Fatalf("error = %q, want no orders", err.Error())
	}
}

func TestValidateBenchmarkLoadAllowsOrdersWithFailures(t *testing.T) {
	err := validateBenchmarkLoad(&model.Metrics{
		OrdersSent:    100,
		ConnectErrors: 2,
	})
	if err != nil {
		t.Fatalf("validateBenchmarkLoad() error = %v", err)
	}
}
