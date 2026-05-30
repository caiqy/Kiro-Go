# SSE 流式标签分割器优化 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 用最小尾巴保留的流式标签分割器替换两个流式 handler 中重复的"50/15 固定缓冲"逻辑，消除 SSE 输出不流畅。

**Architecture:** 新建 `proxy/stream_splitter.go`，提供一个带状态的 `streamTagSplitter`，通过 `emit(text, state)` 回调注入差异。`handleClaudeStream` 和 `handleOpenAIStream` 各自构造一个实例，分别注入 `sendText` / `sendChunk`，替换原 `processClaudeText` / `processText` 闭包。标签拆分语义（内联 `<thinking>` 检测、reasoning-event 与标签源互斥、`dropThinking` 丢弃）保持不变，仅把缓冲粒度由"攒满 50 rune + 留 15 rune 尾巴"改为"只保留可能构成半截标签的最小尾巴"。

**Tech Stack:** Go 1.x，标准库 `strings`，`go test`。

---

## File Structure

- **Create** `proxy/stream_splitter.go` — `streamTagSplitter` 结构体 + `tagPrefixSuffixLen` 辅助函数。职责单一：把 OnText 流切分为正文/思考片段并即时下发。
- **Create** `proxy/stream_splitter_test.go` — 分割器与辅助函数的单元/回归测试。
- **Modify** `proxy/handler.go` — `handleClaudeStream`（~939-1218）与 `handleOpenAIStream`（~1551-1840）：删除局部缓冲状态变量与 `processClaudeText`/`processText` 闭包，改用 `streamTagSplitter`。
- **Unchanged**（原样复用）：`thinkingStreamSource` / `allowTagSource` / `allowReasoningSource`（`proxy/handler.go:43-67`）。`handleResponsesStream` 不涉及。

---

## 语义参照（来自现有 processClaudeText / processText，逐字一致）

emit 的 `state` 含义：`0`=正文文本，`1`=思考开始，`2`=思考续写，`3`=思考结束。

`feed(text, isThinking)` 等价于原 `processX(text, isThinking, false)`：
1. `isThinking && !thinkingEnabled` → return。
2. `isThinking == true`：`allowReasoningSource(&source)` 为 false 则 return；否则首次 `emit(text,1)` 并置 `thinkingStarted=true, eventThinkingOpen=true`，后续 `emit(text,2)`；return。
3. 非思考文本：若 `eventThinkingOpen` 先 `emit("",3)` 并清 `eventThinkingOpen=false, thinkingStarted=false`。
4. `buf += text`，进入循环切分（见下）。

`flush()` 等价于原 `processX("", false, true)`：循环切分时 forceFlush 路径，把残留 buf 全部吐出并复位标签状态。

`closeEventThinking()` 等价于原流末尾的 `if eventThinkingOpen { emit("",3) }`：流结束后单独关闭 reasoning-event 引起的思考块（forceFlush 路径不处理 eventThinkingOpen，故需独立方法）。

### 切分循环（feed 用 force=false，flush 用 force=true）

```
for {
  if !inThinking {
    i := index(buf, "<thinking>")
    if i != -1 {
      if i > 0 { emit(buf[:i], 0) }
      buf = buf[i+10:]; inThinking = true
      dropThinking = !allowTagSource(&source); thinkingStarted = false
    } else if force {
      if buf != "" { emit(buf, 0); buf = "" }
      break
    } else {
      keep := tagPrefixSuffixLen(buf, "<thinking>")  // 末尾可能是半截 <thinking>
      if len(buf) > keep { emit(buf[:len(buf)-keep], 0); buf = buf[len(buf)-keep:] }
      break
    }
  } else {
    j := index(buf, "</thinking>")
    if j != -1 {
      content := buf[:j]
      if !dropThinking {
        if !thinkingStarted { emit(content,1); emit("",3) } else { emit(content,3) }
      }
      buf = buf[j+11:]; inThinking = false; dropThinking = false; thinkingStarted = false
    } else if force {
      if buf != "" {
        if !dropThinking {
          if !thinkingStarted { emit(buf,1); emit("",3) } else { emit(buf,3) }
        }
        buf = ""
      }
      inThinking = false; dropThinking = false; thinkingStarted = false
      break
    } else {
      keep := tagPrefixSuffixLen(buf, "</thinking>")  // 末尾可能是半截 </thinking>
      if len(buf) > keep {
        sendLen := len(buf) - keep
        if !dropThinking {
          if !thinkingStarted { emit(buf[:sendLen],1); thinkingStarted = true } else { emit(buf[:sendLen],2) }
        }
        buf = buf[sendLen:]
      }
      break
    }
  }
}
```

> 注意：与旧实现的差异仅在两个 `else` 分支（原本是 `len(runes)>50 → 留15` 和 `len(runes)>20 → 留15`，按 rune 计）。新实现按字节用 `tagPrefixSuffixLen` 计算保留量。标签为纯 ASCII，保留点必落在 ASCII 边界，不会切坏 UTF-8。其余分支（找到完整标签 / forceFlush）与旧实现逐字一致。

---

## Task 1: 辅助函数 tagPrefixSuffixLen

**Files:**
- Create: `proxy/stream_splitter.go`
- Test: `proxy/stream_splitter_test.go`

- [ ] **Step 1: 写失败测试**

写入 `proxy/stream_splitter_test.go`：

```go
package proxy

import "testing"

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
```

说明：`"<thinking>"` 本身的最长"既是后缀又是前缀"是整串（10）——调用方在调用前已先用 `strings.Index` 处理完整标签命中，故此处返回 10 仅表示"全部可能是标签"，是安全保留，不会漏发正文。`"abc<thin"` 末尾 `<thin`（5 字节）是 `<thinking>` 的前缀。`"secret</think"` 末尾 `</think`（7 字节）是 `</thinking>` 的前缀。

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./proxy/ -run TestTagPrefixSuffixLen -v`
Expected: 编译失败，`undefined: tagPrefixSuffixLen`

- [ ] **Step 3: 实现 tagPrefixSuffixLen**

写入 `proxy/stream_splitter.go`：

```go
package proxy

import "strings"

// tagPrefixSuffixLen 返回 s 的最长后缀的字节长度，使得该后缀同时是 tag 的前缀。
// 用于流式切分：buf 末尾这部分可能是被切断的半截标签，需保留到下一个 chunk 再判定，
// 其余部分可立即下发。tag 均为纯 ASCII，故返回的字节边界一定落在 ASCII 字符上，
// 不会切坏 UTF-8 多字节字符。
func tagPrefixSuffixLen(s, tag string) int {
	max := len(s)
	if len(tag) < max {
		max = len(tag)
	}
	for n := max; n > 0; n-- {
		if strings.HasSuffix(s, tag[:n]) {
			return n
		}
	}
	return 0
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `go test ./proxy/ -run TestTagPrefixSuffixLen -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add proxy/stream_splitter.go proxy/stream_splitter_test.go
git commit -m "feat: add tagPrefixSuffixLen helper for streaming tag detection"
```

---

## Task 2: streamTagSplitter 结构体与切分逻辑

**Files:**
- Modify: `proxy/stream_splitter.go`
- Test: `proxy/stream_splitter_test.go`

- [ ] **Step 1: 写失败测试**

追加到 `proxy/stream_splitter_test.go`。`rec` 收集 emit 调用，方便断言即时下发与标签拆分：

```go
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
	// 期望：思考块内容 "idea" 以 state1+state3 形式发出，然后 "tail" 以 state0 发出
	got := *rec
	// 找到 state0 的 "tail"
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
	// "before" 作为正文 state0 下发
	if got[0] != (emitCall{"before", 0}) {
		t.Fatalf("expected first emit \"before\" state0, got %#v", got)
	}
	// "after" 作为正文 state0 下发（最后一个 state0）
	last := got[len(got)-1]
	if last != (emitCall{"after", 0}) {
		t.Fatalf("expected last emit \"after\" state0, got %#v", got)
	}
	// secret 不能以正文 state0 出现
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
	// 正文 X 和 Y 仍应作为 state0 出现
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
```

文件顶部需 `import "strings"`（测试用到 `strings.Contains`）。若 `strings` 已导入则复用。

- [ ] **Step 2: 运行测试确认失败**

Run: `go test ./proxy/ -run TestSplitter -v`
Expected: 编译失败，`undefined: streamTagSplitter`

- [ ] **Step 3: 实现 streamTagSplitter**

追加到 `proxy/stream_splitter.go`：

```go
// streamTagSplitter 将上游 OnText 流切分为正文/思考片段并通过 emit 即时下发。
// 它在保证不泄漏被切断的半截 <thinking>/</thinking> 标签的前提下，尽量立即下发文本，
// 避免固定大阈值缓冲导致的流式卡顿。
//
// emit 的 state 含义：0=正文文本，1=思考开始，2=思考续写，3=思考结束。
type streamTagSplitter struct {
	thinkingEnabled bool
	emit            func(text string, state int)

	buf            string
	inThinking     bool
	dropThinking   bool
	thinkingStarted bool
	eventThinkingOpen bool
	source         thinkingStreamSource
}

// feed 处理一段上游文本。isThinking 表示该段来自 reasoningContentEvent。
func (s *streamTagSplitter) feed(text string, isThinking bool) {
	if isThinking && !s.thinkingEnabled {
		return
	}

	if isThinking {
		if !allowReasoningSource(&s.source) {
			return
		}
		if !s.thinkingStarted {
			s.emit(text, 1)
			s.thinkingStarted = true
			s.eventThinkingOpen = true
		} else {
			s.emit(text, 2)
		}
		return
	}

	if s.eventThinkingOpen {
		s.emit("", 3)
		s.eventThinkingOpen = false
		s.thinkingStarted = false
	}

	s.buf += text
	s.split(false)
}

// flush 在流结束或工具调用前强制吐出残留 buffer。
func (s *streamTagSplitter) flush() {
	s.split(true)
}

// closeEventThinking 在流结束后关闭由 reasoningContentEvent 打开的思考块。
// flush 的 force 路径不处理 eventThinkingOpen，故单独提供。
func (s *streamTagSplitter) closeEventThinking() {
	if s.eventThinkingOpen {
		s.emit("", 3)
		s.eventThinkingOpen = false
		s.thinkingStarted = false
	}
}

func (s *streamTagSplitter) split(force bool) {
	for {
		if !s.inThinking {
			i := strings.Index(s.buf, "<thinking>")
			if i != -1 {
				if i > 0 {
					s.emit(s.buf[:i], 0)
				}
				s.buf = s.buf[i+10:]
				s.inThinking = true
				s.dropThinking = !allowTagSource(&s.source)
				s.thinkingStarted = false
			} else if force {
				if s.buf != "" {
					s.emit(s.buf, 0)
					s.buf = ""
				}
				break
			} else {
				keep := tagPrefixSuffixLen(s.buf, "<thinking>")
				if len(s.buf) > keep {
					s.emit(s.buf[:len(s.buf)-keep], 0)
					s.buf = s.buf[len(s.buf)-keep:]
				}
				break
			}
		} else {
			j := strings.Index(s.buf, "</thinking>")
			if j != -1 {
				content := s.buf[:j]
				if !s.dropThinking {
					if !s.thinkingStarted {
						s.emit(content, 1)
						s.emit("", 3)
					} else {
						s.emit(content, 3)
					}
				}
				s.buf = s.buf[j+11:]
				s.inThinking = false
				s.dropThinking = false
				s.thinkingStarted = false
			} else if force {
				if s.buf != "" {
					if !s.dropThinking {
						if !s.thinkingStarted {
							s.emit(s.buf, 1)
							s.emit("", 3)
						} else {
							s.emit(s.buf, 3)
						}
					}
					s.buf = ""
				}
				s.inThinking = false
				s.dropThinking = false
				s.thinkingStarted = false
				break
			} else {
				keep := tagPrefixSuffixLen(s.buf, "</thinking>")
				if len(s.buf) > keep {
					sendLen := len(s.buf) - keep
					if !s.dropThinking {
						if !s.thinkingStarted {
							s.emit(s.buf[:sendLen], 1)
							s.thinkingStarted = true
						} else {
							s.emit(s.buf[:sendLen], 2)
						}
					}
					s.buf = s.buf[sendLen:]
				}
				break
			}
		}
	}
}
```

- [ ] **Step 4: 运行测试确认通过**

Run: `go test ./proxy/ -run TestSplitter -v`
Expected: PASS（4 个子测试全部通过）

- [ ] **Step 5: 提交**

```bash
git add proxy/stream_splitter.go proxy/stream_splitter_test.go
git commit -m "feat: add streamTagSplitter for low-latency SSE tag splitting"
```

---

## Task 3: 接入 handleClaudeStream

**Files:**
- Modify: `proxy/handler.go`（`handleClaudeStream`，~939-1218）

- [ ] **Step 1: 删除局部缓冲状态变量**

定位 `handleClaudeStream` 内（约 939-944 行）：

```go
		var textBuffer string
		var inThinkingBlock bool
		var dropTagThinking bool
		var thinkingSource thinkingStreamSource
		var thinkingStarted bool
		var eventThinkingOpen bool
```

删除这 6 行（这些状态移入 splitter）。`sendText` 闭包保持不变。

- [ ] **Step 2: 用 splitter 替换 processClaudeText 定义**

删除整个 `processClaudeText := func(text string, isThinking bool, forceFlush bool) {...}` 闭包（约 1028-1132 行），在 `sendText` 闭包之后替换为：

```go
		splitter := &streamTagSplitter{
			thinkingEnabled: thinking,
			emit:            sendText,
		}
```

- [ ] **Step 3: 更新 OnText 调用点**

`callback.OnText`（约 1144 行）中：

```go
				processClaudeText(text, isThinking, false)
```

改为：

```go
				splitter.feed(text, isThinking)
```

- [ ] **Step 4: 更新 OnToolUse 调用点**

`callback.OnToolUse` 开头（约 1147 行）：

```go
				processClaudeText("", false, true)
```

改为：

```go
				splitter.flush()
```

- [ ] **Step 5: 更新流末尾调用点**

请求成功后（约 1214-1217 行）：

```go
		processClaudeText("", false, true)
		if eventThinkingOpen {
			sendText("", 3)
		}
		closeActiveBlock()
```

改为：

```go
		splitter.flush()
		splitter.closeEventThinking()
		closeActiveBlock()
```

- [ ] **Step 6: 编译确认无残留引用**

Run: `go build ./...`
Expected: 成功。若报 `undefined: eventThinkingOpen` 或 `processClaudeText`，说明还有调用点未替换，按报错定位修正。

- [ ] **Step 7: 跑现有 Claude 测试**

Run: `go test ./proxy/ -run TestClaude -v`
Expected: PASS（现有 `handler_test.go` 中 Claude 相关测试不回归）

- [ ] **Step 8: 提交**

```bash
git add proxy/handler.go
git commit -m "refactor: use streamTagSplitter in handleClaudeStream"
```

---

## Task 4: 接入 handleOpenAIStream

**Files:**
- Modify: `proxy/handler.go`（`handleOpenAIStream`，~1551-1840）

- [ ] **Step 1: 删除局部缓冲状态变量**

定位 `handleOpenAIStream` 内（约 1551-1556 行）：

```go
		var textBuffer string
		var inThinkingBlock bool
		var dropTagThinking bool
		var thinkingSource thinkingStreamSource
		var thinkingStarted bool
		var eventThinkingOpen bool
```

删除这 6 行。`sendChunk` 闭包保持不变。

- [ ] **Step 2: 用 splitter 替换 processText 定义**

删除整个 `processText := func(text string, isThinking bool, forceFlush bool) {...}` 闭包（约 1657-1761 行），在 `sendChunk` 闭包之后替换为：

```go
		splitter := &streamTagSplitter{
			thinkingEnabled: thinking,
			emit:            sendChunk,
		}
```

- [ ] **Step 3: 更新 OnText 调用点**

`callback.OnText`（约 1773 行）：

```go
				processText(text, isThinking, false)
```

改为：

```go
				splitter.feed(text, isThinking)
```

- [ ] **Step 4: 更新 OnToolUse 调用点**

`callback.OnToolUse` 开头（约 1776 行）：

```go
				processText("", false, true)
```

改为：

```go
				splitter.flush()
```

- [ ] **Step 5: 更新流末尾调用点**

请求成功后（约 1837-1840 行）：

```go
		processText("", false, true)
		if eventThinkingOpen {
			sendChunk("", 3)
		}
```

改为：

```go
		splitter.flush()
		splitter.closeEventThinking()
```

- [ ] **Step 6: 编译确认无残留引用**

Run: `go build ./...`
Expected: 成功。若报 `declared and not used` 或 `undefined`，按报错定位剩余引用修正。

- [ ] **Step 7: 跑测试**

Run: `go test ./proxy/ -v`
Expected: PASS（含 OpenAI、Claude、Responses、kiro 全部现有测试）

- [ ] **Step 8: 提交**

```bash
git add proxy/handler.go
git commit -m "refactor: use streamTagSplitter in handleOpenAIStream"
```

---

## Task 5: 全量验证

**Files:** 无（仅验证）

- [ ] **Step 1: 全量编译**

Run: `go build ./...`
Expected: 成功，无错误。

- [ ] **Step 2: 全量测试**

Run: `go test ./...`
Expected: 全部 PASS。

- [ ] **Step 3: vet 检查**

Run: `go vet ./...`
Expected: 无告警（如有既有告警与本次无关则忽略）。

- [ ] **Step 4: 确认无残留旧符号**

用搜索确认 `processClaudeText`、`processText`（作为闭包名）、以及 `handleClaudeStream`/`handleOpenAIStream` 内的 `textBuffer` 均已移除。

Expected: 这些标识符不再出现在两个流式函数体内（`textBuffer` 等若在其它函数另有同名局部变量则不影响）。

---

## 自检

- **Spec 覆盖**：核心算法替换（Task 1+2）、共享结构抽出（Task 2）、Claude 接入（Task 3）、OpenAI 接入（Task 4）、测试（Task 1/2 内联 + Task 5 全量）。Responses 不涉及，spec 已说明。全部覆盖。
- **占位符**：无 TBD/TODO，所有代码步骤给出完整代码。
- **类型一致性**：`streamTagSplitter` 字段名（`thinkingEnabled`/`emit`/`buf`/`inThinking`/`dropThinking`/`thinkingStarted`/`eventThinkingOpen`/`source`）与方法名（`feed`/`flush`/`closeEventThinking`/`split`）在 Task 2 定义、Task 3/4 一致使用。`tagPrefixSuffixLen` 签名 `(s, tag string) int` 全程一致。`thinkingStreamSource`/`allowTagSource`/`allowReasoningSource` 沿用现有定义。
