package unit

import (
	"os"
	"path/filepath"
	"strings"
	"testing"

	eba "github.com/wlz987/eba-go/src"
)

func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	return filepath.Clean(filepath.Join(wd, "..", ".."))
}

func readRepo(t *testing.T, rel string) string {
	t.Helper()
	b, err := os.ReadFile(filepath.Join(repoRoot(t), rel))
	if err != nil {
		t.Fatal(err)
	}
	return string(b)
}

func TestStableSurface(t *testing.T) {
	var (
		_ eba.ActorId
		_ eba.EnvelopeId
		_ *eba.Header
		_ *eba.Envelope
		_ eba.Options
		_ eba.IdGen  = eba.UuidIdGen{}
		_ eba.IdGen  = (*eba.SeqIdGen)(nil)
		_ *eba.Inbox = eba.NewInbox(1)
		_ eba.Pattern
		_ eba.Wildcard = eba.WildcardNone
		_ error        = &eba.InvalidTopic{}
		_ *eba.Bus     = eba.NewBus()
		_ error        = &eba.BusError{}
		_ error        = &eba.MailboxFull{}
		_ *eba.Subscriber
		_ *eba.Publisher
		_ eba.State = eba.StatePending
		_ eba.ResolveOutcome
		_ *eba.Registry = eba.NewRegistry()
		_ eba.StartParams
		_ *eba.Matchmaker
	)
}

func TestAgentLockFacade(t *testing.T) {
	facade := readRepo(t, "src/eba.go")
	for _, stale := range []string{
		"CompleteParked",
		"func Park(",
		"HasResultShape",
		"QuadOK",
		"resolve_or_drop",
		"LooksLikeResultEnv ",
		"= reply.Hold",
	} {
		if strings.Contains(facade, stale) {
			t.Fatalf("facade still exports stale %q", stale)
		}
	}
	if !strings.Contains(facade, "NewMatchmaker") {
		t.Fatal("facade must export NewMatchmaker")
	}
}

func TestAgentLockInternalsUnexported(t *testing.T) {
	queue := readRepo(t, "src/jobhost/queue.go")
	if strings.Contains(queue, "func NewEnvelopeQueue") || strings.Contains(queue, "type EnvelopeQueue") {
		t.Fatal("开工队列不得作为 jobhost 导出类型")
	}
	if !strings.Contains(queue, "func newEnvelopeQueue") {
		t.Fatal("queue ctor must be package-private")
	}
	body := readRepo(t, "src/result/body.go")
	if strings.Contains(body, "func HasResultShape") {
		t.Fatal("HasResultShape must not be exported")
	}
	quad := readRepo(t, "src/registry/quad.go")
	if strings.Contains(quad, "func QuadOK") {
		t.Fatal("QuadOK must not be exported")
	}
}

func TestAgentLockDispatchOrphanFinish(t *testing.T) {
	src := readRepo(t, "src/jobhost/dispatch.go")
	if !strings.Contains(src, "FinishSafe") {
		t.Fatal("orphan/echo path must FinishSafe")
	}
	if strings.Contains(src, "resolve_or_drop") {
		t.Fatal("no second resolve route")
	}
}

func TestAgentLockNoStaleSrc(t *testing.T) {
	stale := []string{
		"CompleteParked",
		"func Park(",
		"DropJob",
		"func (j *Job) Publish(",
		"resolve_or_drop",
		"HasResultShape",
		"func (j *Job) rejectIdle(",
		"func (s *SlotBook) Drop(",
	}
	srcRoot := filepath.Join(repoRoot(t), "src")
	err := filepath.Walk(srcRoot, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() || !strings.HasSuffix(path, ".go") {
			return nil
		}
		b, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		text := string(b)
		rel, _ := filepath.Rel(srcRoot, path)
		for _, token := range stale {
			if strings.Contains(text, token) {
				t.Errorf("%s still has %q", rel, token)
			}
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

func TestAgentLockPrinciples(t *testing.T) {
	principles := readRepo(t, "PRINCIPLES.md")
	agentPath := filepath.Join(repoRoot(t), "..", "AGENT.md")
	if b, err := os.ReadFile(agentPath); err == nil {
		if principles != string(b) {
			t.Fatal("PRINCIPLES.md must match workspace AGENT.md")
		}
	}
	readme := readRepo(t, "README.md")
	for _, p := range []string{
		"设计简约", "实现丰富", "最小实现下界", "软约束",
		"接口克制", "克制导入导出", "克制暴露", "克制大文件",
		"规避死代码", "规避内部冲突", "全局路线唯一", "ResolveOnly",
		"语言差不是第二套会合", "Job 两句", "延迟应答", "Matchmaker", "本轮不做",
	} {
		if !strings.Contains(readme, p) {
			t.Fatalf("README missing %q", p)
		}
	}
}
