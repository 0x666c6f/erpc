package thirdparty

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
	"strings"

	"github.com/erpc/erpc/common"
)

const (
	vendorSettingProviderID = "__erpc_provider_id"
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

	h := sha256.New()
	writeCachePart(h, "vendor", vendorName)
	writeCachePart(h, "provider", providerID)
	for _, part := range parts {
		writeCachePart(h, "part", part)
	}

	return fmt.Sprintf("%s|provider=%s|key=%s", vendorName, providerID, hex.EncodeToString(h.Sum(nil))[:24])
}

type cacheHash interface {
	Write([]byte) (int, error)
}

func writeCachePart(h cacheHash, name, value string) {
	_, _ = h.Write([]byte(name))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(strconv.Itoa(len(value))))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write([]byte(value))
	_, _ = h.Write([]byte{0})
}

func cachePart(name string, value string) string {
	return name + "=" + value
}

func secretCachePart(name string, value string) string {
	if value == "" {
		return name + "="
	}
	sum := sha256.Sum256([]byte(value))
	return name + "=sha256:" + hex.EncodeToString(sum[:])[:24]
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
