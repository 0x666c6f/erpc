package simulator

import (
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestYAMLHighlightEscapesValueHTML(t *testing.T) {
	node, err := exec.LookPath("node")
	if err != nil {
		t.Skip("node not installed")
	}

	_, file, _, ok := runtime.Caller(0)
	require.True(t, ok)
	repoRoot := findRepoRoot(t, filepath.Dir(file))
	highlightPath := filepath.Join(repoRoot, "cmd/erpc-simulator/web/yaml-util.js")

	script := `
const fs = require("fs");
const vm = require("vm");
const code = fs.readFileSync(process.argv[1], "utf8");
const ctx = { window: {} };
vm.createContext(ctx);
vm.runInContext(code, ctx);
const out = ctx.window.YAMLU.highlightLine("endpoint: <img src=x onerror=alert(1)>");
if (out.includes("<img")) {
  throw new Error("raw img tag rendered: " + out);
}
if (!out.includes("&lt;img") || !out.includes("&gt;")) {
  throw new Error("escaped payload missing: " + out);
}
const tilde = ctx.window.YAMLU.highlightLine("empty: ~");
if (!tilde.includes('<span class="tk-bool">~</span>')) {
  throw new Error("tilde token was not highlighted: " + tilde);
}
`
	cmd := exec.Command(node, "-e", script, highlightPath)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
}

func findRepoRoot(t *testing.T, start string) string {
	t.Helper()

	dir := start
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			return dir
		}
		parent := filepath.Dir(dir)
		require.NotEqual(t, dir, parent, "repository root not found from %s", start)
		dir = parent
	}
}
