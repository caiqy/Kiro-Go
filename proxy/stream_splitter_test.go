package proxy

import (
	"strings"
	"testing"
)

func TestTagPrefixSuffixLen(t *testing.T) {
	cases := []struct {
		name string
		s    string
		tag  string
		want int
	}{
		{"empty string", "", "<thinking>", 0},
		{"no angle bracket", "hello world", "<thinking>", 0},
		{"full tag is not a partial suffix", "<thinking>", "<thinking>", 10},
		{"half open tag", "abc<thin", "<thinking>", 5},
		{"single lt", "abc<", "<thinking>", 1},
		{"half close tag", "secret</think", "</thinking>", 7},
		{"lt in middle only", "a<b c", "<thinking>", 0},
		{"trailing lt after text", "done<", "</thinking>", 1},
		{"longer than tag", "xx<thinking>yy", "<thinking>", 0},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := tagPrefixSuffixLen(c.s, c.tag); got != c.want {
				t.Fatalf("tagPrefixSuffixLen(%q,%q)=%d, want %d", c.s, c.tag, got, c.want)
			}
		})
	}
}

type emitCall struct {
	text  string
	state int
}

func newRecSplitter(thinking bool) (*streamTagSplitter, *[]emitCall) {
	var rec []emitCall
	s := &streamTagSplitter{
		thinkingEnabled: thinking,
		emit: func(text string, state int) {
			rec = append(rec, emitCall{text, state})
		},
	}
	return s, &rec
}

// 核心 bug 回归：无标签短文本必须立即整段下发，不得憋住。
func TestSplitterImmediatePlainText(t *testing.T) {
	s, rec := newRecSplitter(false)
	s.feed("Hello", false)
	if len(*rec) != 1 || (*rec)[0] != (emitCall{"Hello", 0}) {
		t.Fatalf("expected immediate emit of \"Hello\" state 0, got %#v", *rec)
	}
	s.feed(" world", false)
	if len(*rec) != 2 || (*rec)[1] != (emitCall{" world", 0}) {
		t.Fatalf("expected immediate emit of \" world\", got %#v", *rec)
	}
}

// 末尾出现可能是半截标签的 '<' 时，保留该尾巴，其余立即下发。
func TestSplitterHoldsPartialTagSuffix(t *testing.T) {
	s, rec := newRecSplitter(true)
	s.feed("abc<thin", false)
	// 应只发出 "abc"，保留 "<thin"
	if len(*rec) != 1 || (*rec)[0] != (emitCall{"abc", 0}) {
		t.Fatalf("expected emit \"abc\" only, got %#v", *rec)
	}
	// 续上 "king>" 构成完整 <thinking>，再喂思考内容与闭合
	s.feed("king>idea</thinking>tail", false)
	got := *rec
	last := got[len(got)-1]
	if last != (emitCall{"tail", 0}) {
		t.Fatalf("expected last emit \"tail\" state0, got %#v", got)
	}
	sawThinking := false
	for _, c := range got {
		if c.state == 1 && c.text == "idea" {
			sawThinking = true
		}
	}
	if !sawThinking {
		t.Fatalf("expected thinking content \"idea\" emitted at state1, got %#v", got)
	}
}

// 跨 chunk 分割的开标签也能正确识别。
func TestSplitterCrossChunkOpenTag(t *testing.T) {
	s, rec := newRecSplitter(true)
	s.feed("before<thin", false)
	s.feed("king>secret</thinking>after", false)
	s.flush()
	got := *rec
	if got[0] != (emitCall{"before", 0}) {
		t.Fatalf("expected first emit \"before\" state0, got %#v", got)
	}
	last := got[len(got)-1]
	if last != (emitCall{"after", 0}) {
		t.Fatalf("expected last emit \"after\" state0, got %#v", got)
	}
	for _, c := range got {
		if c.state == 0 && strings.Contains(c.text, "secret") {
			t.Fatalf("thinking content leaked into plain text: %#v", got)
		}
	}
}

// dropThinking：当 source 已是 reasoning-event 时，标签块内容应被丢弃但标签被剥离，正文不受影响。
func TestSplitterDropThinkingWhenReasoningSourceActive(t *testing.T) {
	s, rec := newRecSplitter(true)
	// 先来一个 reasoning-event，锁定 source 为 reasoningEvent
	s.feed("reasoning", true)
	// 再来正文里内联的 <thinking> 块，应被 drop（不发 thinking，也不发其内容）
	s.feed("X<thinking>dropme</thinking>Y", false)
	s.flush()
	got := *rec
	for _, c := range got {
		if strings.Contains(c.text, "dropme") {
			t.Fatalf("dropped thinking content was emitted: %#v", got)
		}
	}
	var plain string
	for _, c := range got {
		if c.state == 0 {
			plain += c.text
		}
	}
	if plain != "XY" {
		t.Fatalf("expected plain text \"XY\", got %q (all=%#v)", plain, got)
	}
}

// flush 在 reasoning-event 思考块打开时（如 reasoning 后紧跟 tool call）必须关闭该块，
// 等价于旧实现 processX("",false,true) 开头的 eventThinkingOpen 收尾逻辑。
func TestSplitterFlushClosesOpenReasoningBlock(t *testing.T) {
	s, rec := newRecSplitter(true)
	s.feed("thinking...", true) // 打开 reasoning 思考块 (state 1)
	s.flush()                   // 模拟 OnToolUse：思考后直接调工具
	got := *rec
	if len(got) == 0 {
		t.Fatalf("expected emits, got none")
	}
	// 必须出现一次思考闭合 (state 3)
	closed := false
	for _, c := range got {
		if c.state == 3 {
			closed = true
		}
	}
	if !closed {
		t.Fatalf("expected thinking block to be closed (state 3) on flush, got %#v", got)
	}
	// 状态需复位：后续再来 reasoning 应以"开始"(state 1) 处理，而非"续写"(state 2)
	s.feed("more", true)
	got = *rec
	last := got[len(got)-1]
	if last != (emitCall{"more", 1}) {
		t.Fatalf("expected reopened thinking to start at state 1, got %#v", last)
	}
}
