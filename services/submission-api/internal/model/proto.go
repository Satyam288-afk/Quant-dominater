package model

import benchmarkv1 "github.com/iicpc/benchmark-platform/shared/go/benchmark/v1"

func (s *Submission) ToProto() *benchmarkv1.Submission {
	if s == nil {
		return nil
	}
	return &benchmarkv1.Submission{
		SubmissionId:  s.SubmissionID,
		TeamId:        s.TeamID,
		Language:      s.Language,
		Protocol:      s.Protocol,
		Artifact:      s.Artifact.ToProto(),
		CreatedAtUnix: s.CreatedAtUnix,
	}
}

func (a SubmissionArtifact) ToProto() *benchmarkv1.SubmissionArtifact {
	return &benchmarkv1.SubmissionArtifact{
		ArtifactId: a.ArtifactID,
		Uri:        a.URI,
		Sha256:     a.SHA256,
		SizeBytes:  a.SizeBytes,
	}
}

func (s SandboxSpec) ToProto() *benchmarkv1.SandboxSpec {
	return &benchmarkv1.SandboxSpec{
		CpuLimit:      s.CPULimit,
		MemoryLimit:   s.MemoryLimit,
		NetworkEgress: s.NetworkEgress,
	}
}

func (c BenchmarkRunConfig) ToProto() *benchmarkv1.BenchmarkRunConfig {
	return &benchmarkv1.BenchmarkRunConfig{
		BotCount:       int32(c.BotCount),
		RatePerBot:     int32(c.RatePerBot),
		DurationSec:    int32(c.DurationSec),
		WarmupSec:      int32(c.WarmupSec),
		EngineEndpoint: c.EngineEndpoint,
	}
}

func (r *BenchmarkRun) ToProto() *benchmarkv1.BenchmarkRun {
	if r == nil {
		return nil
	}
	return &benchmarkv1.BenchmarkRun{
		RunId:         r.RunID,
		SubmissionId:  r.SubmissionID,
		TeamId:        r.TeamID,
		Status:        runStatusToProto(r.Status),
		BenchmarkSeed: r.BenchmarkSeed,
		Sandbox:       r.Sandbox.ToProto(),
		Config:        r.Config.ToProto(),
		CreatedAtUnix: r.CreatedAtUnix,
		UpdatedAtUnix: r.UpdatedAtUnix,
	}
}

func runStatusToProto(status RunStatus) benchmarkv1.RunStatus {
	if value, ok := benchmarkv1.RunStatus_value[string(status)]; ok {
		return benchmarkv1.RunStatus(value)
	}
	return benchmarkv1.RunStatus_RUN_STATUS_UNSPECIFIED
}
