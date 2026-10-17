package main

import (
	"log/slog"

	"github.com/KimMachineGun/automemlimit/memlimit"
)

// goMemoryLimitRatio is the share of the container memory limit the Go runtime
// treats as its soft memory limit. The garbage collector works harder as the heap
// approaches it, so the process collects before the kernel's cgroup ceiling
// OOM-kills it; the remaining tenth is headroom for what a collection cannot
// return on demand. It is a GC target, not a guarantee against the ceiling.
const goMemoryLimitRatio = 0.9

// goMemoryLimitSource reports the effective container memory limit: the smallest
// limit on the process's cgroup hierarchy, or memlimit.ErrNoLimit when the
// hierarchy sets none.
var goMemoryLimitSource memlimit.Provider = memlimit.FromCgroup

// applyGoMemoryLimit derives the Go soft memory limit from the container limit,
// once, at startup. An operator's GOMEMLIMIT wins and nothing is derived;
// AUTOMEMLIMIT replaces the ratio or, set to "off", disables the derivation. A
// container with no limit leaves the runtime explicitly unlimited. Failing to
// read the limit is never fatal: it is GC tuning, the kernel ceiling still
// applies, and the runtime keeps the limit it already had. The library reports
// every outcome through log, so the result is not inspected here.
func applyGoMemoryLimit(log *slog.Logger) {
	_, _ = memlimit.Set(
		memlimit.WithRatio(goMemoryLimitRatio),
		memlimit.WithLogger(log),
		memlimit.WithProvider(goMemoryLimitSource),
	)
}
