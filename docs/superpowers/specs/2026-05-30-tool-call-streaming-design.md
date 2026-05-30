# 工具调用增量流式 优化设计

日期: 2026-05-30

## 问题

写文件等工具调用（tool use）在流式请求下"等很久、一次性输出"。

### 根因（已用服务器埋点确认）

- 上游 Kiro API 把工具调用的 input 以**字符串片段**增量发送：多个 `toolUseEvent`，每个带一小段 `input`（几到十几字节），片段拼接起来正好是完整的 JSON 文本。
- kiro-go 的 `handleToolUseEvent`（`proxy/kiro.go`）把这些片段全部累加进 `InputBuffer`，**只在收到 `stop=true` 时**才调用一次 `finishToolUse` → `OnToolUse`，带着完整 input。
- 三个流式 handler 的 `OnToolUse` 拿到完整 input 后，用**单个** `input_json_delta`（Claude/Responses）或单个 tool_calls chunk（OpenAI）一次性发出。

### 实测证据（1500 行写文件）

- 服务器侧：上游用 **2208 个增量片段、约 87 秒**流式吐出整个文件（约 30KB）。
- 客户端侧：只收到 **1 个** `input_json_delta`。
- 客户端体验：约 4 秒等待 + 约 87 秒静默 + 最后一瞬间整个文件弹出。
- 行数越多越严重；150 行时上游约 0.7 秒传完，流式收益小，但大文件收益显著。
- 跨境网络开销很小（首 token 约 0.8 秒、全程约 0.2 秒），不是卡顿主因。

## 目标

把工具调用从"攒齐再一次性发"改成"边收边转"，对齐 Anthropic 原生三段式：
`content_block_start` → 多个 `input_json_delta` → `content_block_stop`。
覆盖三个流式端点：Claude (`/v1/messages`)、OpenAI (`/v1/chat/completions`)、Responses (`/v1/responses`)。

## 方案

### 1. 回调层改造（proxy/kiro.go）

`KiroStreamCallback` 新增两个**可选**回调，保留现有 `OnToolUse` 作为"完成/记账"信号：

```go
type KiroStreamCallback struct {
    OnText         func(text string, isThinking bool)
    OnToolUseStart func(toolUseID, name string) // 新增：工具块开始
    OnToolUseDelta func(toolUseID, partialJSON string) // 新增：input 增量片段
    OnToolUse      func(toolUse KiroToolUse)    // 保留：stop 时触发，带完整 input，用于记账/历史/最终内容
    OnComplete     func(inputTokens, outputTokens int)
    OnError        func(err error)
    OnCredits      func(credits float64)
    OnContextUsage func(percentage float64)
}
```

**向后兼容（关键约束）**：当 `OnToolUseStart` / `OnToolUseDelta` 均为 nil 时，`handleToolUseEvent` 行为与现状完全一致——攒齐后只触发一次 `OnToolUse(完整)`。非流式 handler（`handleClaudeNonStream`、`handleOpenAINonStream`、`handleResponsesNonStream`）、`apiTestAccount` 不设增量回调，行为不变。现有测试 `TestParseEventStream*` 必须继续通过。

### 2. handleToolUseEvent 增量触发（proxy/kiro.go）

`toolUseState` 增加 `Started bool` 与 `EmittedDelta bool` 状态。

新增辅助函数 `startToolUseIfNeeded(state, callback)`：当 `state.Started` 为 false、`Name` 非空、`ToolUseID` 非空、设置了 `OnToolUseStart`，**且 `GeneratedID` 为 false** 时，触发一次 `OnToolUseStart(toolUseID, name)` 并置 `Started=true`。对 `GeneratedID` 工具（上游先给 name、后给真实 ID）刻意**不**在此触发，避免 Start 用到的临时 ID 与最终真实 ID 不一致——它们的 Start 推迟到 `finishToolUse` 兜底。

`handleToolUseEvent` 处理 input 时：

- 先调 `startToolUseIfNeeded`。
- 收到 string 片段 `input`：照常 `InputBuffer.WriteString(input)`；**仅当 `OnToolUseDelta` 已设置且 `state.Started == true`** 时才触发 `OnToolUseDelta(toolUseID, 片段)` 并置 `EmittedDelta=true`。`Started == false` 时（即 GeneratedID 工具尚未 Start）**不发增量、不置 EmittedDelta**，留待兜底补发完整 partial_json，保证参数不丢且 Start 必先于 Delta。
- 收到 map 快照形态 `input`（上游偶发，非字符串增量）：照常 `InputBuffer.Reset()` 重写 buffer；**不**逐片转发（无法安全增量），留待 stop 兜底。
- `stop=true` → `finishToolUse`：
  - 解析完整 buffer 得到 `input` map。
  - **Start 兜底**：若设置了 `OnToolUseStart` 但 `Started == false`（GeneratedID 或 map 快照路径），先补发 `OnToolUseStart`，置 `Started=true`。
  - **Delta 兜底**：若设置了 `OnToolUseDelta` 但 `EmittedDelta == false` 且 buffer 非空，补发一个 `OnToolUseDelta(toolUseID, 完整JSON)`，保证客户端拿到完整参数。
  - 触发 `OnToolUse(完整 KiroToolUse)`（记账/历史/最终内容，所有 handler 都用）。

工具切换（当前工具未 stop 又来新工具）时，先对旧工具走完成流程（含上面的兜底与 OnToolUse），再开始新工具。

**GeneratedID 工具的最终行为**：因 Start 与 Delta 都推迟到 `finishToolUse`，这类工具会以"Start + 单个完整 Delta + Stop"的形式发出（不是逐片增量，但参数完整、顺序正确，等同改动前的体验，不退化）。带显式 `toolUseId` 的工具（探针实测的真实上游形态）则正常逐片增量。

### 3. CallKiroAPI 名称还原扩展（proxy/kiro.go）

`CallKiroAPI` 现在用 `nameMap` wrap `OnToolUse` 还原工具原名。需扩展：也 wrap `OnToolUseStart`（其 name 参数同样要还原）。`OnToolUseDelta` 只含 toolUseID + partial_json，无需还原。

### 4. 三个流式 handler 拆分（proxy/handler.go、proxy/responses_handler.go）

每个 handler 把现在"一次性发完整工具块"的 `OnToolUse` 拆成三段回调：

**Claude（handler.go，handleClaudeStream）**
- `OnToolUseStart(id, name)`：`splitter.flush()`；`ensureMessageStart()`；`closeActiveBlock()`；分配 `idx := nextContentIndex++`；记录 `idx` 供 delta 使用；发 `content_block_start`（type=tool_use, id, name, input={}）。
- `OnToolUseDelta(id, pj)`：发 `content_block_delta`（type=input_json_delta, partial_json=pj），index=记录的 idx。
- `OnToolUse(tu)`：发 `content_block_stop`（index=idx）；`rawContentBuilder` 累加 name+input（用于 token 估算/历史）；`toolUses = append(toolUses, tu)`。

**OpenAI（handler.go，handleOpenAIStream）**
- `OnToolUseStart(id, name)`：`splitter.flush()`；发首个 tool_calls chunk（index=toolCallIndex, id, type=function, function.name=name, function.arguments=""）。
- `OnToolUseDelta(id, pj)`：发 tool_calls chunk（index=toolCallIndex, function.arguments=pj），不带 name/id。
- `OnToolUse(tu)`：`toolCalls = append(...)`（用于最终统计）；`rawContentBuilder` 累加；`toolCallIndex++`。

**Responses（responses_handler.go，handleResponsesStream）**
- `OnToolUseStart(id, name)`：若 messageStarted 先收尾 message item（content_part.done + output_item.done，messageStarted=false，outputIndex++）；分配 `fcID`；发 `response.output_item.added`（function_call, status=in_progress, call_id=id, name, arguments=""）。
- `OnToolUseDelta(id, pj)`：发 `response.function_call_arguments.delta`（item_id=fcID, delta=pj）。
- `OnToolUse(tu)`：发 `response.output_item.done`（function_call, status=completed, arguments=完整JSON）；`toolUses = append(...)`；outputIndex++。

handler 内用局部变量在 Start 时记录当前工具的 idx/fcID，供 Delta 和最终 stop 使用。

### 5. 边界与正确性

- **Start 必先于 Delta（关键不变量）**：handler 的 `OnToolUseDelta` 依赖 Start 已建立块上下文（Claude 的 `toolBlockIndex`、Responses 的 `curFcID`）。若 Delta 早于 Start 到达，Claude/Responses 会因守卫丢弃该 Delta、OpenAI 会发出无前导 start 的畸形块 → 工具参数丢失。因此 string 片段的 Delta 必须加 `state.Started` 守卫（见第 2 节）。
- **GeneratedID 降级**：仅 name、无显式 toolUseId 的工具，Start/Delta 都推迟到 `finishToolUse`，以"Start + 单个完整 Delta + Stop"发出，参数完整不丢，不退化。
- **map 快照降级**：协议仍正确（兜底补发完整 delta），体验等同改动前。
- **多工具调用**：带显式 ID 的多工具各自独立 start/delta/stop 分段，index/fcID 不串；name 切换的多工具中第二个走 GeneratedID 降级路径。
- **partial_json 合法性**：上游 string 片段拼接即完整 JSON，单个片段本身可能不是合法 JSON（这正是 input_json_delta 的设计——客户端累加后再解析），符合 Anthropic 协议。
- **非流式与 apiTestAccount**：不设增量回调，走兼容路径，零行为变化。

## 测试

`proxy/kiro_test.go` 增加（实际实现）：

- `TestHandleToolUseEventStreamsIncrementally`：喂入多个 string 片段的 toolUseEvent + stop，断言 `OnToolUseStart` 一次 → 多个 `OnToolUseDelta`（拼接 == 完整 JSON）→ `OnToolUse` 一次（完整 input）。
- `TestHandleToolUseEventMapSnapshotFallsBackToSingleDelta`：input 为 map 形态 + stop，断言增量模式下补发**恰好一个**完整 `OnToolUseDelta` 后再 `OnToolUse`。
- `TestHandleToolUseEventMultipleToolsWithExplicitIDsStreamEach`：两个带显式 ID 的工具连续调用，断言完整事件序列 `A start/delta/delta/use, B start/delta/delta/use`，两个工具都逐片增量、ID 不混。
- `TestHandleToolUseEventMultipleToolsByNameSwitchStreamEach`：仅靠 name 变化切换的两个工具，断言第二个工具 Start 先于 Use。
- `TestHandleToolUseEventStartPrecedesDeltaForGeneratedID`：GeneratedID 工具的严格顺序不变量——首个回调必须是 `OnToolUseStart` 而非 `OnToolUseDelta`。**此测试在评审阶段暴露了一个真实 bug**（早期实现中 GeneratedID 工具的 Delta 先于 Start 触发，导致 handler 丢弃 Delta、参数丢失），修复即第 2 节的 `state.Started` 守卫。
- 现有 `TestParseEventStreamFinishesPendingToolUseOnEOF`、`TestParseEventStreamNilCallback*`、`TestHandleToolUseEventGeneratesMissingToolUseID`、`TestHandleToolUseEventReplacesGeneratedIDWhenRealIDArrives` 继续通过（向后兼容）。

运行 `go build ./...`、`go test ./...`、`go vet ./...` 全绿。

## 改动面与风险

- 修改：`proxy/kiro.go`（回调结构、`toolUseState`、`handleToolUseEvent`、新增 `startToolUseIfNeeded`、`finishToolUse`、CallKiroAPI wrap）、`proxy/handler.go`（Claude+OpenAI 两个流式 handler 的 callback）、`proxy/responses_handler.go`（Responses 流式 handler 的 callback）、`proxy/kiro_test.go`（新增测试）。
- 不涉及非流式路径。属于协议行为变更（工具参数由单块变多块增量），但映射到三个协议都自然，且对客户端是协议兼容的（客户端本就该累加 input_json_delta）。
- 效果：大文件工具调用逐步实时出现，消除"静默等待后一次性弹出"。模型生成耗时与网络固有延迟不受影响（无法通过本改动消除）。
- **评审修复**：实现并评审后发现 GeneratedID 工具的 Delta 早于 Start 触发会导致 handler 丢弃参数，已通过 `state.Started` 守卫修复（commit `b60e0f1`），并补充多工具与顺序不变量测试。
