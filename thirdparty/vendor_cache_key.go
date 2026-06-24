package thirdparty

import (
	"fmt"
	"hash/maphash"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"

	"github.com/erpc/erpc/common"
)

const (
	vendorSettingProviderID = "__erpc_provider_id"
)

var (
	secretCachePartIDs sync.Map
	secretCachePartSeq atomic.Uint64
	vendorCacheKeySeed = maphash.MakeSeed()
)

func withProviderSettings(settings common.VendorSettings, providerID string) common.VendorSettings {
	copied := make(common.VendorSettings, len(settings)+1)
	for k, v := range settings {
		copied[k] = v
	}
	copied[vendorSettingProviderID] = providerID
	return copied
}

func vendorSettingString(settings common.VendorSettings, key string) string {
	if settings == nil {
		return ""
	}
	if value, ok := settings[key].(string); ok {
		return value
	}
	return ""
}

func vendorCapabilityCacheKey(vendorName string, settings common.VendorSettings, parts ...string) string {
	providerID := vendorSettingString(settings, vendorSettingProviderID)
	if providerID == "" {
		providerID = "standalone"
	}

	var b strings.Builder
	writeCachePart(&b, "vendor", vendorName)
	writeCachePart(&b, "provider", providerID)
	for _, part := range parts {
		writeCachePart(&b, "part", part)
	}

	return fmt.Sprintf("%s|provider=%s|key=%016x", vendorName, providerID, maphash.String(vendorCacheKeySeed, b.String()))
}

func writeCachePart(b *strings.Builder, name, value string) {
	b.WriteString(name)
	b.WriteByte(0)
	b.WriteString(strconv.Itoa(len(value)))
	b.WriteByte(0)
	b.WriteString(value)
	b.WriteByte(0)
}

func cachePart(name string, value string) string {
	return name + "=" + value
}

func secretCachePart(name string, value string) string {
	if value == "" {
		return name + "="
	}
	key := name + "\x00" + value
	if cached, ok := secretCachePartIDs.Load(key); ok {
		return name + "=secret:" + cached.(string)
	}

	generated := strconv.FormatUint(secretCachePartSeq.Add(1), 36)
	actual, _ := secretCachePartIDs.LoadOrStore(key, generated)
	return name + "=secret:" + actual.(string)
}

func intSliceCachePart(name string, values []int) string {
	copied := append([]int(nil), values...)
	sort.Ints(copied)
	out := make([]string, len(copied))
	for i, value := range copied {
		out[i] = strconv.Itoa(value)
	}
	return name + "=" + strings.Join(out, ",")
}

func stringSliceCachePart(name string, values []string) string {
	copied := append([]string(nil), values...)
	sort.Strings(copied)
	return name + "=" + strings.Join(copied, ",")
}
