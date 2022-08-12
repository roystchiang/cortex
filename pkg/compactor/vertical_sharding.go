package compactor

import (
	"bytes"
	"container/heap"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
)

func NewVerticalCompactingChunkSeriesMerger(mergeFunc storage.VerticalSeriesMergeFunc) storage.VerticalChunkSeriesMergeFunc {
	return func(series ...storage.ChunkSeries) storage.ChunkSeries {
		if len(series) == 0 {
			return nil
		}

		//do some sharding
		//labelSet := series[0].Labels()
		//if labelSet.Hash() % 2 == 0 {
		//	return &storage.ChunkSeriesEntry{
		//		Lset: series[0].Labels(),
		//		ChunkIteratorFn: func() chunks.Iterator {
		//			iterators := make([]chunks.Iterator, 0, len(series))
		//			return &compactChunkIterator{
		//				mergeFunc: mergeFunc,
		//				iterators: iterators,
		//			}
		//		},
		//	}
		//}

		return &storage.ChunkSeriesEntry{
			Lset: series[0].Labels(),
			ChunkIteratorFn: func() chunks.Iterator {
				iterators := make([]chunks.Iterator, 0, len(series))
				for _, s := range series {
						iterators = append(iterators, s.Iterator())
				}
				return &compactChunkIterator{
					mergeFunc: mergeFunc,
					iterators: iterators,
				}
			},
		}
	}
}

type compactChunkIterator struct {
	mergeFunc storage.VerticalSeriesMergeFunc
	iterators []chunks.Iterator

	h chunkIteratorHeap

	err  error
	curr chunks.Meta
}

func (c *compactChunkIterator) At() chunks.Meta {
	return c.curr
}

func (c *compactChunkIterator) Next() bool {
	if len(c.iterators) == 0 {
		return false
	}
	if c.h == nil {
		for _, iter := range c.iterators {
			if iter.Next() {
				heap.Push(&c.h, iter)
			}
		}
	}
	if len(c.h) == 0 {
		return false
	}

	iter := heap.Pop(&c.h).(chunks.Iterator)
	c.curr = iter.At()
	if iter.Next() {
		heap.Push(&c.h, iter)
	}

	var (
		overlapping []storage.Series
		oMaxTime    = c.curr.MaxTime
		prev        = c.curr
	)
	// Detect overlaps to compact. Be smart about it and deduplicate on the fly if chunks are identical.
	for len(c.h) > 0 {
		// Get the next oldest chunk by min, then max time.
		next := c.h[0].At()
		if next.MinTime > oMaxTime {
			// No overlap with current one.
			break
		}

		if next.MinTime == prev.MinTime &&
			next.MaxTime == prev.MaxTime &&
			bytes.Equal(next.Chunk.Bytes(), prev.Chunk.Bytes()) {
			// 1:1 duplicates, skip it.
		} else {
			// We operate on same series, so labels does not matter here.
			overlapping = append(overlapping, newChunkToSeriesDecoder(nil, next))
			if next.MaxTime > oMaxTime {
				oMaxTime = next.MaxTime
			}
			prev = next
		}

		iter := heap.Pop(&c.h).(chunks.Iterator)
		if iter.Next() {
			heap.Push(&c.h, iter)
		}
	}
	if len(overlapping) == 0 {
		return true
	}

	// Add last as it's not yet included in overlap. We operate on same series, so labels does not matter here.
	iter = storage.NewSeriesToChunkEncoder(c.mergeFunc(append(overlapping, newChunkToSeriesDecoder(nil, c.curr))...)).Iterator()
	if !iter.Next() {
		if c.err = iter.Err(); c.err != nil {
			return false
		}
		panic("unexpected seriesToChunkEncoder lack of iterations")
	}
	c.curr = iter.At()
	if iter.Next() {
		heap.Push(&c.h, iter)
	}
	return true
}

func (c *compactChunkIterator) Err() error {
	errs := tsdb_errors.NewMulti()
	for _, iter := range c.iterators {
		errs.Add(iter.Err())
	}
	errs.Add(c.err)
	return errs.Err()
}

type chunkIteratorHeap []chunks.Iterator

func (h chunkIteratorHeap) Len() int      { return len(h) }
func (h chunkIteratorHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }

func (h chunkIteratorHeap) Less(i, j int) bool {
	at := h[i].At()
	bt := h[j].At()
	if at.MinTime == bt.MinTime {
		return at.MaxTime < bt.MaxTime
	}
	return at.MinTime < bt.MinTime
}

func (h *chunkIteratorHeap) Push(x interface{}) {
	*h = append(*h, x.(chunks.Iterator))
}

func (h *chunkIteratorHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

func newChunkToSeriesDecoder(labels labels.Labels, chk chunks.Meta) storage.Series {
	return &storage.SeriesEntry{
		Lset: labels,
		SampleIteratorFn: func() chunkenc.Iterator {
			// TODO(bwplotka): Can we provide any chunkenc buffer?
			return chk.Chunk.Iterator(nil)
		},
	}
}