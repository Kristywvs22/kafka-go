package kafka

import (
	"sync"
)

type ReaderConfig struct {
	// ... existing fields
	AllowOutOfOrderCommits bool
	Logger                 Logger
}

type offsetTracker struct {
	mu      sync.Mutex
	offsets map[int]map[int64]struct{}
}

func (t *offsetTracker) track(partition int, offset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	if t.offsets == nil { t.offsets = make(map[int]map[int64]struct{}) }
	if t.offsets[partition] == nil { t.offsets[partition] = make(map[int64]struct{}) }
	t.offsets[partition][offset] = struct{}{}
}

func (t *offsetTracker) check(partition int, offset int64) bool {
	t.mu.Lock()
	defer t.mu.Unlock()
	for o := range t.offsets[partition] {
		if o < offset { return true }
	}
	return false
}

func (t *offsetTracker) remove(partition int, offset int64) {
	t.mu.Lock()
	defer t.mu.Unlock()
	delete(t.offsets[partition], offset)
}

// ... existing Reader struct and methods

func (r *Reader) CommitMessages(ctx context.Context, msgs ...Message) error {
	for _, msg := range msgs {
		if !r.config.AllowOutOfOrderCommits && r.tracker.check(msg.Partition, msg.Offset) {
			r.config.Logger.Printf("Warning: Committing offset %d before lower offset(s) have been committed. This may lead to silent message loss.", msg.Offset)
		}
		r.tracker.remove(msg.Partition, msg.Offset)
	}
	return r.commitMessages(ctx, msgs...)
}