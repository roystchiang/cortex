package compactor

import (
	"context"
	"fmt"
	"github.com/oklog/ulid"
	"github.com/thanos-io/thanos/pkg/compact"

	"github.com/go-kit/log"
	"github.com/thanos-io/thanos/pkg/block/metadata"
)

type ShuffleShardingPlanner struct {
	logger log.Logger
	ranges []int64
	noCompBlocksFunc func() map[ulid.ULID]*metadata.NoCompactMark
}

func NewShuffleShardingPlanner(logger log.Logger, ranges []int64, noCompBlocks *compact.GatherNoCompactionMarkFilter) *ShuffleShardingPlanner {
	return &ShuffleShardingPlanner{
		logger: logger,
		ranges: ranges,
		noCompBlocksFunc: noCompBlocks.NoCompactMarkedBlocks,
	}
}

func (p *ShuffleShardingPlanner) Plan(_ context.Context, metasByMinTime []*metadata.Meta) ([]*metadata.Meta, error) {
	// Ensure all blocks fits within the largest range. This is a double check
	// to ensure there's no bug in the previous blocks grouping, given this Plan()
	// is just a pass-through.
	// Modifed from https://github.com/cortexproject/cortex/pull/2616/files#diff-e3051fc530c48bb276ba958dd8fadc684e546bd7964e6bc75cef9a86ef8df344R28-R63
	largestRange := p.ranges[len(p.ranges)-1]
	rangeStart := getRangeStart(metasByMinTime[0], largestRange)
	rangeEnd := rangeStart + largestRange

	noCompactMarked := p.noCompBlocksFunc()
	for _, b := range metasByMinTime {
		if _, excluded := noCompactMarked[b.ULID]; excluded {
			p.logger.Log("Excluded compaction block", b.ULID)
			return nil, nil
		}
		if b.MinTime < rangeStart || b.MaxTime > rangeEnd {
			return nil, fmt.Errorf("block %s with time range %d:%d is outside the largest expected range %d:%d", b.ULID.String(), b.MinTime, b.MaxTime, rangeStart, rangeEnd)
		}
	}

	return metasByMinTime, nil
}
