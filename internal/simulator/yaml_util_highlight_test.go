package simulator

import (
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
	repoRoot := filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
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
`
	cmd := exec.Command(node, "-e", script, highlightPath)
	out, err := cmd.CombinedOutput()
	require.NoError(t, err, string(out))
}
