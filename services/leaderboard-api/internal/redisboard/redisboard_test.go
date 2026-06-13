package redisboard

import "testing"

func TestDeriveAvgLatencyMs(t *testing.T) {
	cases := []struct {
		name string
		in   map[string]string
		want string
	}{
		{
			name: "derives avg from sum and count (ns -> ms)",
			// 30_000_000 ns total over 3 samples = 10_000_000 ns avg = 10 ms.
			in:   map[string]string{"latency_sum_ns": "30000000", "latency_count": "3"},
			want: "10",
		},
		{
			name: "zero count yields zero (no divide-by-zero)",
			in:   map[string]string{"latency_sum_ns": "1234", "latency_count": "0"},
			want: "0",
		},
		{
			name: "missing fields yield zero",
			in:   map[string]string{"orders_sent": "5"},
			want: "0",
		},
		{
			name: "unparsable count yields zero",
			in:   map[string]string{"latency_sum_ns": "100", "latency_count": "abc"},
			want: "0",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			deriveAvgLatencyMs(tc.in)
			if got := tc.in["avg_latency_ms"]; got != tc.want {
				t.Fatalf("avg_latency_ms = %q, want %q", got, tc.want)
			}
		})
	}
}

func TestDeriveAvgLatencyMsNilMapNoPanic(t *testing.T) {
	deriveAvgLatencyMs(nil) // must not panic
}
