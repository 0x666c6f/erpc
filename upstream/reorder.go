package upstream

import (
	"sort"
	"strings"
)

// ReorderUpstreams enforces a preferred upstream ID order in registry snapshots.
// It is mainly used by tests that need deterministic routing order.
func ReorderUpstreams(reg *UpstreamsRegistry, preferredOrder ...string) {
	if reg == nil || reg.upstreamsMu == nil {
		return
	}

	reg.upstreamsMu.Lock()
	defer reg.upstreamsMu.Unlock()

	if len(preferredOrder) == 0 {
		ids := make(map[string]struct{})
		for _, methodMap := range reg.sortedUpstreams {
			for _, upsSlice := range methodMap {
				for _, ups := range upsSlice {
					ids[ups.Id()] = struct{}{}
				}
			}
		}
		for _, upsSlice := range reg.networkUpstreams {
			for _, ups := range upsSlice {
				ids[ups.Id()] = struct{}{}
			}
		}
		preferredOrder = make([]string, 0, len(ids))
		for id := range ids {
			preferredOrder = append(preferredOrder, id)
		}
		sort.Strings(preferredOrder)
	}

	for _, methodMap := range reg.sortedUpstreams {
		for method, upsSlice := range methodMap {
			methodMap[method] = reorderSliceOfUpstreams(upsSlice, preferredOrder)
		}
	}
	for networkID, upsSlice := range reg.networkUpstreams {
		reordered := reorderSliceOfUpstreams(upsSlice, preferredOrder)
		reg.networkUpstreams[networkID] = reordered
		cp := make([]*Upstream, len(reordered))
		copy(cp, reordered)
		reg.networkUpstreamsAtomic.Store(networkID, cp)
	}
}

func reorderSliceOfUpstreams(upsSlice []*Upstream, preferredOrder []string) []*Upstream {
	if len(upsSlice) < 2 || len(preferredOrder) == 0 {
		return upsSlice
	}

	preferredIndex := make(map[string]int, len(preferredOrder))
	for i, id := range preferredOrder {
		preferredIndex[strings.TrimSpace(id)] = i
	}

	type entry struct {
		up    *Upstream
		index int
		pos   int
		found bool
	}
	items := make([]entry, len(upsSlice))
	for i, ups := range upsSlice {
		pos, found := preferredIndex[ups.Id()]
		items[i] = entry{up: ups, index: i, pos: pos, found: found}
	}
	sort.SliceStable(items, func(i, j int) bool {
		if items[i].found && items[j].found {
			return items[i].pos < items[j].pos
		}
		if items[i].found != items[j].found {
			return items[i].found
		}
		return items[i].index < items[j].index
	})

	out := make([]*Upstream, len(items))
	for i := range items {
		out[i] = items[i].up
	}
	return out
}
