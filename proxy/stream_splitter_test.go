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

// 副作用核对：byte 切分下，UTF-8 多字节字符不得被切坏。
// 跨 chunk 喂入含中文/emoji 的文本，拼回的正文必须与原文逐字节相同（无乱码、无丢字）。
func TestSplitterPreservesMultibyteUTF8(t *testing.T) {
	const full = "你好世界🌍这是一段中文测试abc"
	// 逐字节喂入（最极端的切分，会把每个多字节字符拆散到多个 chunk）
	s, rec := newRecSplitter(false)
	for i := 0; i < len(full); i++ {
		s.feed(full[i:i+1], false)
	}
	s.flush()
	var assembled string
	for _, c := range *rec {
		if c.state == 0 {
			assembled += c.text
		}
	}
	if assembled != full {
		t.Fatalf("UTF-8 corrupted: got %q, want %q", assembled, full)
	}
}

// 副作用核对：chunk 边界无关性。
// 同一输入用不同切法喂入，渲染成逻辑段（正文段 / 思考块，按内容拼接）后必须一致。
// state1/state2 的分界是 chunk 粒度决定的（新旧代码皆然，消费端 state1 发开标记+文本、
// state2 仅发文本，拼接后渲染相同），故归一化时把同一思考块的 state1/2/3 折叠为一个块。
// 这证明本次重构唯一改变的是流式粒度，不改变正文内容、思考内容与块边界归属。
func TestSplitterChunkBoundaryInvariance(t *testing.T) {
	inputs := []string{
		"plain text only no tags",
		"before<thinking>inner thoughts</thinking>after",
		"a<thinking>t1</thinking>b<thinking>t2</thinking>c",
		"text with a lone < bracket and more",
		"trailing partial <thin",
		"<thinking>only thinking no close",
		"你好<thinking>思考内容</thinking>世界",
	}

	type seg struct {
		kind string // "body" 或 "think"
		text string
	}

	// render：把 emit 序列折叠为逻辑段，消除 state1/state2 的 chunk 粒度差异。
	render := func(calls []emitCall) []seg {
		var out []seg
		appendTo := func(kind, text string) {
			if len(out) > 0 && out[len(out)-1].kind == kind {
				out[len(out)-1].text += text
			} else {
				out = append(out, seg{kind, text})
			}
		}
		for _, c := range calls {
			switch c.state {
			case 0:
				if c.text != "" {
					appendTo("body", c.text)
				}
			case 1:
				out = append(out, seg{"think", c.text}) // 块开始：总是起新块
			case 2:
				if c.text != "" {
					appendTo("think", c.text)
				}
			case 3:
				if c.text != "" {
					appendTo("think", c.text)
				}
			}
		}
		return out
	}

	feedAllAtOnce := func(in string) []seg {
		s, rec := newRecSplitter(true)
		s.feed(in, false)
		s.flush()
		return render(*rec)
	}

	feedByRunes := func(in string) []seg {
		s, rec := newRecSplitter(true)
		for _, r := range in {
			s.feed(string(r), false)
		}
		s.flush()
		return render(*rec)
	}

	feedByChunks := func(in string, size int) []seg {
		s, rec := newRecSplitter(true)
		runes := []rune(in)
		for i := 0; i < len(runes); i += size {
			end := i + size
			if end > len(runes) {
				end = len(runes)
			}
			s.feed(string(runes[i:end]), false)
		}
		s.flush()
		return render(*rec)
	}

	equal := func(a, b []seg) bool {
		if len(a) != len(b) {
			return false
		}
		for i := range a {
			if a[i] != b[i] {
				return false
			}
		}
		return true
	}

	for _, in := range inputs {
		ref := feedAllAtOnce(in)
		if got := feedByRunes(in); !equal(ref, got) {
			t.Fatalf("rune-by-rune differs for %q:\n once=%#v\n rune=%#v", in, ref, got)
		}
		if got := feedByChunks(in, 3); !equal(ref, got) {
			t.Fatalf("3-rune-chunk differs for %q:\n once=%#v\n chunk=%#v", in, ref, got)
		}
	}
}
