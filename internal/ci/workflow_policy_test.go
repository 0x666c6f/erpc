package ci

import (
	"os"
	"path/filepath"
	"runtime"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

type workflowFile struct {
	On   map[string]any         `yaml:"on"`
	Jobs map[string]workflowJob `yaml:"jobs"`
}

type workflowJob struct {
	If          any            `yaml:"if"`
	Environment any            `yaml:"environment"`
	Outputs     map[string]any `yaml:"outputs"`
}

func repoRoot(t *testing.T) string {
	t.Helper()

	_, file, _, ok := runtime.Caller(0)
	if !ok {
		t.Fatal("failed to resolve caller")
	}
	return filepath.Clean(filepath.Join(filepath.Dir(file), "../.."))
}

func readWorkflow(t *testing.T, rel string) (string, workflowFile) {
	t.Helper()

	raw, err := os.ReadFile(filepath.Join(repoRoot(t), rel))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}

	var wf workflowFile
	if err := yaml.Unmarshal(raw, &wf); err != nil {
		t.Fatalf("parse %s: %v", rel, err)
	}
	return string(raw), wf
}

func TestReleaseWorkflowPublishesOnlyFromReleaseTags(t *testing.T) {
	text, wf := readWorkflow(t, ".github/workflows/release.yml")

	push, ok := wf.On["push"].(map[string]any)
	if !ok {
		t.Fatalf("release workflow must declare push trigger, got %#v", wf.On["push"])
	}
	if _, ok := push["branches"]; ok {
		t.Fatal("release workflow must not publish from branch pushes")
	}
	tags, ok := push["tags"].([]any)
	if !ok || len(tags) != 1 || tags[0] != "*.*.*" {
		t.Fatalf("release workflow must publish only from semver-like tags, got %#v", push["tags"])
	}
	if strings.Contains(text, "refs/heads/morpho-main") || strings.Contains(text, "head_commit.message") {
		t.Fatal("release workflow must not gate publishing on branch names or commit messages")
	}
	if strings.Contains(text, "Create tag") || strings.Contains(text, "git push origin ${{ steps.version.outputs.VERSION }}") {
		t.Fatal("release workflow must not create or force-push release tags itself")
	}
	if strings.Contains(text, "(branch)") || strings.Contains(text, "digest_main") {
		t.Fatal("release workflow must not keep branch-image publish paths")
	}

	for _, name := range []string{
		"release",
		"docker-build-amd64",
		"docker-build-arm64",
		"docker-manifest",
		"docker-cleanup-sha256-tags",
	} {
		job, ok := wf.Jobs[name]
		if !ok {
			t.Fatalf("missing release job %q", name)
		}
		if got := environmentName(job.Environment); got != "release" {
			t.Fatalf("%s must use release environment, got %q", name, got)
		}
		if cond := stringify(job.If); !strings.Contains(cond, "github.ref_type == 'tag'") {
			t.Fatalf("%s must require tag refs, got %q", name, cond)
		}
	}
}

func TestBenchmarkWorkflowRejectsForkPRCode(t *testing.T) {
	text, _ := readWorkflow(t, ".github/workflows/benchmark.yml")

	for _, want := range []string{
		"head_is_fork=true",
		"Reject fork PR benchmarks",
		"Refusing to benchmark fork PR code on privileged runners",
		"git fetch upstream morpho-main",
		"git checkout upstream/morpho-main",
	} {
		if !strings.Contains(text, want) {
			t.Fatalf("benchmark workflow missing policy marker %q", want)
		}
	}
}

func TestSecurityWorkflowRunsPolicyTests(t *testing.T) {
	text, _ := readWorkflow(t, ".github/workflows/test.yml")

	if !strings.Contains(text, "Check workflow security policy") ||
		!strings.Contains(text, "go test ./internal/ci -count=1 -v") {
		t.Fatal("security workflow must run workflow policy tests")
	}
}

func TestCLIInstallerVerifiesChecksumBeforeExecutable(t *testing.T) {
	raw, err := os.ReadFile(filepath.Join(repoRoot(t), "typescript/cli/src/install.ts"))
	if err != nil {
		t.Fatalf("read installer: %v", err)
	}
	text := string(raw)

	verifyIdx := strings.Index(text, "await verifyChecksum(binaryPath, platform);")
	chmodIdx := strings.Index(text, "await chmod(binaryPath, 0o755);")
	if verifyIdx == -1 {
		t.Fatal("installer must verify downloaded binary checksum")
	}
	if chmodIdx == -1 {
		t.Fatal("installer chmod call missing")
	}
	if verifyIdx > chmodIdx {
		t.Fatal("installer must verify checksum before making binary executable")
	}
}

func environmentName(env any) string {
	switch v := env.(type) {
	case string:
		return v
	case map[string]any:
		if name, ok := v["name"].(string); ok {
			return name
		}
	}
	return ""
}

func stringify(v any) string {
	switch value := v.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(strings.ReplaceAll(strings.ReplaceAll(value, "\n", " "), "\t", " "))
	default:
		return ""
	}
}
