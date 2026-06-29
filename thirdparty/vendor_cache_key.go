package thirdparty

import (
	"context"
	"crypto/hmac"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"hash/maphash"
	"sort"
	"strconv"
	"strings"
)

type vendorProviderIDContextKey struct{}

var (
	vendorCacheKeySeed = maphash.MakeSeed()
	secretCacheKey     = newSecretCacheKey()
)

func newSecretCacheKey() []byte {
	key := make([]byte, 32)
	if _, err := rand.Read(key); err != nil {
		panic(fmt.Errorf("generate vendor secret cache key: %w", err))
	}
	return key
}

func contextWithProviderID(ctx context.Context, providerID string) context.Context {
	if providerID == "" {
		return ctx
	}
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, vendorProviderIDContextKey{}, providerID)
}

func providerIDFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	if providerID, ok := ctx.Value(vendorProviderIDContextKey{}).(string); ok {
		return providerID
	}
	return ""
}

func vendorCapabilityCacheKey(vendorName string, providerID string, parts ...string) string {
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
	mac := hmac.New(sha256.New, secretCacheKey)
	_, _ = fmt.Fprintf(mac, "%s\x00%d\x00%s", name, len(value), value)
	return name + "=secret:" + hex.EncodeToString(mac.Sum(nil))[:24]
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
