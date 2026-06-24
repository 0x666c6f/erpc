package simulator

import (
	"bufio"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestDumperLogRequestCompactsRawJSONForJSONL(t *testing.T) {
	path := filepath.Join(t.TempDir(), "simulator.jsonl")
	d, err := NewDumper(path)
	require.NoError(t, err)

	d.LogRequest(&TraceEvent{
		ID:        1,
		StartedAt: time.Unix(0, 0).UTC(),
		Method:    "eth_chainId",
		Outcome:   "ok",
	}, json.RawMessage("[1,\n2]"), json.RawMessage(`{"jsonrpc":"2.0","params":[1,
2]}`))
	require.NoError(t, d.Close())

	f, err := os.Open(path)
	require.NoError(t, err)
	defer f.Close()

	var requestLines []string
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		var payload struct {
			Kind string `json:"kind"`
		}
		require.NoError(t, json.Unmarshal([]byte(line), &payload))
		if payload.Kind == "request" {
			requestLines = append(requestLines, line)
		}
	}
	require.NoError(t, scanner.Err())
	require.Len(t, requestLines, 1, "request event must stay one JSONL record")
	require.Contains(t, requestLines[0], `"params":[1,2]`)
	require.Contains(t, requestLines[0], `"body":{"jsonrpc":"2.0","params":[1,2]}`)
}
