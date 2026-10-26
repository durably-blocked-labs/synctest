package resourcegc

import (
	"reflect"
	"runtime"
)

// Stable observation identifiers. Keep existing values when adding markers so
// saved timelines retain their meaning. These are not application actions or
// scheduling decisions.
const (
	TagWorkerEnter   uint32 = 1
	TagWorkerReturn  uint32 = 2
	TagCleanupEnter  uint32 = 3
	TagCleanupReturn uint32 = 4
	TagClaimEnter    uint32 = 5
	TagClaimReturn   uint32 = 6
	TagReadEnter     uint32 = 7
	TagReadReturn    uint32 = 8
	TagComputeEnter  uint32 = 9
	TagComputeReturn uint32 = 10
	TagCommitEnter   uint32 = 11
	TagCommitReturn  uint32 = 12
	TagReleaseEnter  uint32 = 13
	TagReleaseReturn uint32 = 14
	TagCheckEnter    uint32 = 15
	TagCheckReturn   uint32 = 16
	TagCloseEnter    uint32 = 17
	TagCloseReturn   uint32 = 18
)

// CallTagInfo names an explicitly instrumented function boundary. File and Line
// identify the function entry, not the exact marker or a GC suspension point.
type CallTagInfo struct {
	Tag       uint32 `json:"tag"`
	Actor     string `json:"actor"`
	Operation string `json:"operation"`
	Phase     string `json:"phase"`
	Function  string `json:"function"`
	File      string `json:"file"`
	Line      int    `json:"line"`
}

// LookupCallTag resolves metadata after execution, outside the observer path.
// Exact suspension PCs, if recorded by the runtime, are separate observations.
func LookupCallTag(tag uint32) (CallTagInfo, bool) {
	info := CallTagInfo{Tag: tag}
	var function any
	switch tag {
	case TagWorkerEnter, TagWorkerReturn:
		info.Actor, info.Operation, function = "worker", "runWorker", (*resource).runWorker
	case TagCleanupEnter, TagCleanupReturn:
		info.Actor, info.Operation, function = "cleanup", "runCleanup", (*resource).runCleanup
	case TagClaimEnter, TagClaimReturn:
		info.Actor, info.Operation, function = "worker", "claim", (*resource).claim
	case TagReadEnter, TagReadReturn:
		info.Actor, info.Operation, function = "worker", "read", (*resource).read
	case TagComputeEnter, TagComputeReturn:
		info.Actor, info.Operation, function = "worker", "compute", (*resource).compute
	case TagCommitEnter, TagCommitReturn:
		info.Actor, info.Operation, function = "worker", "commit", (*resource).commit
	case TagReleaseEnter, TagReleaseReturn:
		info.Actor, info.Operation, function = "worker", "release", (*resource).release
	case TagCheckEnter, TagCheckReturn:
		info.Actor, info.Operation, function = "cleanup", "check", (*resource).check
	case TagCloseEnter, TagCloseReturn:
		info.Actor, info.Operation, function = "cleanup", "close", (*resource).close
	default:
		return CallTagInfo{}, false
	}
	if tag%2 == 1 {
		info.Phase = "enter"
	} else {
		info.Phase = "return"
	}
	if fn := runtime.FuncForPC(reflect.ValueOf(function).Pointer()); fn != nil {
		info.Function = fn.Name()
		info.File, info.Line = fn.FileLine(fn.Entry())
	}
	return info, true
}
