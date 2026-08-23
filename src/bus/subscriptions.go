package bus

import (
	"sort"

	"github.com/wlz987/eba-go/src/inbox"
	"github.com/wlz987/eba-go/src/pattern"
)

type subKey struct {
	inboxID uint64
	pat     string
}

type subEntry struct {
	seq int
	pat pattern.Pattern
	box *inbox.Inbox
}

type Subscriptions struct {
	byFirst  map[string]map[subKey]*subEntry
	catchAll map[subKey]*subEntry
	byInbox  map[uint64]map[subKey]struct{}
	nextSeq  int
}

func NewSubscriptions() *Subscriptions {
	return &Subscriptions{
		byFirst:  map[string]map[subKey]*subEntry{},
		catchAll: map[subKey]*subEntry{},
		byInbox:  map[uint64]map[subKey]struct{}{},
	}
}

func (s *Subscriptions) Subscribe(pat pattern.Pattern, box *inbox.Inbox) {
	key := subKey{box.ID(), pat.Key()}
	bucket := s.writableBucket(pat)
	if _, ok := bucket[key]; ok {
		return
	}
	bucket[key] = &subEntry{seq: s.nextSeq, pat: pat, box: box}
	s.nextSeq++
	set, ok := s.byInbox[box.ID()]
	if !ok {
		set = map[subKey]struct{}{}
		s.byInbox[box.ID()] = set
	}
	set[key] = struct{}{}
}

func (s *Subscriptions) Unsubscribe(pat pattern.Pattern, box *inbox.Inbox) bool {
	key := subKey{box.ID(), pat.Key()}
	bucket := s.bucket(pat)
	if bucket == nil {
		return false
	}
	if _, ok := bucket[key]; !ok {
		return false
	}
	delete(bucket, key)
	if len(pat.Literal) > 0 && len(bucket) == 0 {
		delete(s.byFirst, pat.Literal[0])
	}
	if keys, ok := s.byInbox[box.ID()]; ok {
		delete(keys, key)
		if len(keys) == 0 {
			delete(s.byInbox, box.ID())
		}
	}
	return true
}

func (s *Subscriptions) SnapshotMatch(topicParts []string) []*inbox.Inbox {
	type hit struct {
		box *inbox.Inbox
		seq int
	}
	earliest := map[uint64]hit{}
	collect := func(entries map[subKey]*subEntry) {
		for _, entry := range entries {
			if !pattern.Matches(entry.pat, topicParts) {
				continue
			}
			id := entry.box.ID()
			if prev, ok := earliest[id]; ok && prev.seq <= entry.seq {
				continue
			}
			earliest[id] = hit{entry.box, entry.seq}
		}
	}
	if len(topicParts) > 0 {
		if literal := s.byFirst[topicParts[0]]; literal != nil {
			collect(literal)
		}
	}
	if len(s.catchAll) > 0 {
		collect(s.catchAll)
	}
	wins := make([]hit, 0, len(earliest))
	for _, h := range earliest {
		wins = append(wins, h)
	}
	sort.Slice(wins, func(i, j int) bool { return wins[i].seq < wins[j].seq })
	boxes := make([]*inbox.Inbox, 0, len(wins))
	for _, h := range wins {
		boxes = append(boxes, h.box)
	}
	return boxes
}

func (s *Subscriptions) bucket(pat pattern.Pattern) map[subKey]*subEntry {
	if len(pat.Literal) == 0 {
		return s.catchAll
	}
	return s.byFirst[pat.Literal[0]]
}

func (s *Subscriptions) writableBucket(pat pattern.Pattern) map[subKey]*subEntry {
	if len(pat.Literal) == 0 {
		return s.catchAll
	}
	bucket, ok := s.byFirst[pat.Literal[0]]
	if !ok {
		bucket = map[subKey]*subEntry{}
		s.byFirst[pat.Literal[0]] = bucket
	}
	return bucket
}
