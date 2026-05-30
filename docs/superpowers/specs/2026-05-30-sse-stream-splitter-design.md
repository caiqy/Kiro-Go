# SSE 流式输出不流畅 — 标签缓冲优化设计

日期: 2026-05-30

## 问题

反代 Claude 模型时，SSE 流式输出不流畅：会憋一会儿然后突然吐出一大段内容。

### 根因（已确认）

不流畅来自客户端**发送侧**的标签缓冲逻辑，而非上游读取。

- 上游 `parseEventStream`（`proxy/kiro.go:408`）逐 event 读取、无 bufio 缓冲，每个 `assistantResponseEvent` 立即触发 `OnText`，本身是流畅的。
- 客户端侧两个流式分支存在**逐字符完全相同**的一段缓冲逻辑：
  - `handleClaudeStream` 内的 `processClaudeText`（`proxy/handler.go:1028`），emit 函数为 `sendText`。
  - `handleOpenAIStream` 内的 `processText`（`proxy/handler.go:1657`），emit 函数为 `sendChunk`。
- 该逻辑对**所有正文**生效（不只 thinking 请求），目的是检测正文里内联的 `<thinking>...</thinking>` 标签并切成 thinking 块，同时避免把半截标签泄漏给客户端。
- 当前实现用"固定大阈值 + 固定大尾巴"：正文要攒够 **50 个 rune** 才 flush，且每次还留 **15 个 rune** 不发。这就是"憋一会儿、突然吐一大段"的根因。

### 影响面

- `/v1/messages`（含 `/messages`、`/anthropic/v1/messages`）→ `handleClaudeMessages` → `handleClaudeStream`：受影响。
- `/v1/chat/completions`（含 `/chat/completions`）→ `handleOpenAIStream`：受影响。
- `/v1/responses` → `handleResponsesStream`：其 `OnText` 直发、不带此 buffer，**不受影响**。

## 方案

抽出共享的带状态分割器，一次修复 + 消除重复（当前两份是 100% 拷贝，否则同一 bug 要在两处各修且需保持同步）。

### 1. 核心修复：替换缓冲算法

将"攒满 50 rune + 固定留 15 rune 尾巴"替换为**最小尾巴保留**：每次喂入都尽量立即透传，只在 buffer 末尾**可能构成半截标签**时保留那一小段。

判定函数：

```go
// tagPrefixSuffixLen 返回 s 的"最长后缀且同时是 tag 前缀"的字节数。
// 流式时只保留这么多尾字节，跨 chunk 分割的标签仍能识别，其余立即下发。
func tagPrefixSuffixLen(s, tag string) int
```

- 正文模式（未在 thinking 块内）：先找完整 `<thinking>`；没找到则只保留末尾匹配 `<thinking>` 前缀的部分（≤10 字节），其余立即发出。
- thinking 块内：先找完整 `</thinking>`；没找到则只保留末尾匹配 `</thinking>` 前缀的部分（≤11 字节），其余按 thinking 内容发出（受 `dropThinking` 控制）。
- 标签是纯 ASCII，保留点必落在 ASCII 边界，不会切坏 UTF-8 多字节字符，因此按字节切是安全的。
- 效果：上游来多少就发多少（仅减去极小的潜在标签尾巴），逐字流畅。

示例：正文 `"Hello world"`（无 `<`）→ 立即全发；旧逻辑因不足 50 字会一直憋着。

### 2. 抽出共享结构 `streamTagSplitter`

新建 `proxy/stream_splitter.go`，将两份重复逻辑合并为一个带状态的小结构体，差异仅通过 `emit` 回调注入：

```go
// emit 的 state 含义：0=正文文本，1=思考开始，2=思考续写，3=思考结束
type streamTagSplitter struct {
    thinkingEnabled bool
    emit            func(text string, state int)
    // 内部状态：buf / inThinking / dropThinking / thinkingStarted / eventThinkingOpen / source
}

func (s *streamTagSplitter) feed(text string, isThinking bool) // = 原 processX(text, isThinking, false)
func (s *streamTagSplitter) flush()                            // = 原 processX("", false, true)
func (s *streamTagSplitter) closeEventThinking()               // = 原 if eventThinkingOpen { emit("", 3) }
```

保持不变并原样复用：
- `thinkingStreamSource` / `allowTagSource` / `allowReasoningSource`（`proxy/handler.go:43-67`）。
- 内联标签去重、reasoning-event 与标签源互斥、`dropThinking` 丢弃逻辑。

仅替换两个"固定阈值/固定尾巴"分支。

### 3. 接入两个 handler

- `handleClaudeStream`：删除局部缓冲状态变量与 `processClaudeText` 定义，建 `streamTagSplitter{thinkingEnabled: thinking, emit: sendText}`。
  - `processClaudeText(text, isThinking, false)` → `splitter.feed(text, isThinking)`
  - `processClaudeText("", false, true)` → `splitter.flush()`（出现在 OnToolUse 与请求结束处）
  - `proxy/handler.go:1215` 的 `if eventThinkingOpen { sendText("", 3) }` → `splitter.closeEventThinking()`
- `handleOpenAIStream`：同样改法，`emit: sendChunk`。

注意 `sendText`/`sendChunk` 内部依赖各自闭包的 SSE 发送状态（如 `activeBlockIndex`、`responseStarted`），保持原样，仅作为 `emit` 注入。

## 测试

新建 `proxy/stream_splitter_test.go`：

- `tagPrefixSuffixLen` 单元用例：无 `<`、半截 `<thin`、半截 `</think`、完整标签、`<` 出现在中间等。
- **即时下发回归**：feed 一段无标签短文本（< 50 字），断言**立即** emit 全部内容（旧逻辑会憋住）。直接验证 bug。
- **跨 chunk 标签**：`"before<thin"` + `"king>secret</thinking>after"` 分两次喂入，断言正文/思考正确分离、标签被剥离。
- **dropThinking 去重回归**：source 先被设为 reasoning-event 后，内联标签内容被丢弃但标签被正确剥离、正文不受影响。
- 运行 `go test ./...` 确认现有测试（`handler_test.go`、`kiro_test.go`、`responses_handler_test.go` 等）不回归。

## 改动面与风险

- 新增：`proxy/stream_splitter.go`、`proxy/stream_splitter_test.go`。
- 修改：`proxy/handler.go` 的 `handleClaudeStream`、`handleOpenAIStream` 两个函数内部。
- 净减重复代码。无对外行为变化（标签拆分语义不变），仅流式粒度变细、更流畅。
