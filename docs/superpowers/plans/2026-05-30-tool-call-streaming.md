# 工具调用增量流式 实现计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 把工具调用（tool use）从"攒齐再一次性发"改成"边收边转"，让大文件写入等工具调用在三个流式端点上逐步实时出现。

**Architecture:** `KiroStreamCallback` 新增可选的 `OnToolUseStart` / `OnToolUseDelta` 回调，`handleToolUseEvent` 在收到上游增量片段时实时触发，`finishToolUse` 在 stop 时做兜底并触发完整的 `OnToolUse`。三个流式 handler 把 `OnToolUse` 拆成 Start/Delta/Stop 三段，映射到各自协议。未设增量回调的路径（非流式、apiTestAccount）行为完全不变。

**Tech Stack:** Go，标准库 `encoding/json`，`go test`。

**关键约束（向后兼容）:** 当 `OnToolUseStart`/`OnToolUseDelta` 均为 nil 时，`handleToolUseEvent`/`finishToolUse` 行为与现状完全一致。现有测试 `TestParseEventStream*`、`TestHandleToolUseEvent*` 必须继续通过。

---

## Task 1: 回调结构与事件增量触发（kiro.go）

**Files:**
- Modify: `proxy/kiro.go`（`KiroStreamCallback` ~234、`toolUseState` ~667、`handleToolUseEvent` ~674、`finishToolUse` ~716）
- Test: `proxy/kiro_test.go`

- [ ] **Step 1: 写失败测试**

追加到 `proxy/kiro_test.go` 末尾：

```go
func TestHandleToolUseEventStreamsIncrementally(t *testing.T) {
	var startCalls []string
	var deltas []string
	var toolUses []KiroToolUse
	cb := &KiroStreamCallback{
		OnToolUseStart: func(id, name string) { startCalls = append(startCalls, id+"|"+name) },
		OnToolUseDelta: func(id, pj string) { deltas = append(deltas, pj) },
		OnToolUse:      func(tu KiroToolUse) { toolUses = append(toolUses, tu) },
	}

	cur := handleToolUseEvent(map[string]interface{}{
		"toolUseId": "toolu_1", "name": "write_file", "input": `{"path":`,
	}, nil, cb)
	cur = handleToolUseEvent(map[string]interface{}{
		"toolUseId": "toolu_1", "input": `"a.txt",`,
	}, cur, cb)
	cur = handleToolUseEvent(map[string]interface{}{
		"toolUseId": "toolu_1", "input": `"content":"hi"}`, "stop": true,
	}, cur, cb)

	if cur != nil {
		t.Fatalf("expected nil after stop")
	}
	if len(startCalls) != 1 || startCalls[0] != "toolu_1|write_file" {
		t.Fatalf("expected one start for toolu_1|write_file, got %#v", startCalls)
	}
	joined := ""
	for _, d := range deltas {
		joined += d
	}
	if joined != `{"path":"a.txt","content":"hi"}` {
		t.Fatalf("delta join mismatch: %q", joined)
	}
	if len(toolUses) != 1 || toolUses[0].Input["path"] != "a.txt" || toolUses[0].Input["content"] != "hi" {
		t.Fatalf("unexpected final tool use: %#v", toolUses)
	}
}

func TestHandleToolUseEventMapSnapshotFallsBackToSingleDelta(t *testing.T) {
	var deltas []string
	var toolUses []KiroToolUse
	cb := &KiroStreamCallback{
		OnToolUseStart: func(id, name string) {},
		OnToolUseDelta: func(id, pj string) { deltas = append(deltas, pj) },
		OnToolUse:      func(tu KiroToolUse) { toolUses = append(toolUses, tu) },
	}
	cur := handleToolUseEvent(map[string]interface{}{
		"toolUseId": "toolu_2", "name": "write_file",
		"input": map[string]interface{}{"path": "b.txt"},
		"stop":  true,
	}, nil, cb)
	if cur != nil {
		t.Fatalf("expected nil after stop")
	}
	if len(deltas) != 1 {
		t.Fatalf("expected exactly one fallback delta, got %d (%#v)", len(deltas), deltas)
	}
	if len(toolUses) != 1 || toolUses[0].Input["path"] != "b.txt" {
		t.Fatalf("unexpected final tool use: %#v", toolUses)
	}
}
```

- [ ] **Step 2: 运行确认失败**

Run: `go test ./proxy/ -run "TestHandleToolUseEventStreamsIncrementally|TestHandleToolUseEventMapSnapshot" -v`
Expected: 编译通过但 FAIL（startCalls/deltas 为空，因为回调尚未被触发）

- [ ] **Step 3: 给回调结构加字段**

`proxy/kiro.go` 的 `KiroStreamCallback`（~234）改为：

```go
type KiroStreamCallback struct {
	OnText         func(text string, isThinking bool)
	OnToolUseStart func(toolUseID, name string)
	OnToolUseDelta func(toolUseID, partialJSON string)
	OnToolUse      func(toolUse KiroToolUse)
	OnComplete     func(inputTokens, outputTokens int)
	OnError        func(err error)
	OnCredits      func(credits float64)
	OnContextUsage func(percentage float64)
}
```

- [ ] **Step 4: 给 toolUseState 加状态字段**

`proxy/kiro.go` 的 `toolUseState`（~667）改为：

```go
type toolUseState struct {
	ToolUseID    string
	Name         string
	InputBuffer  strings.Builder
	GeneratedID  bool
	Started      bool
	EmittedDelta bool
}
```

- [ ] **Step 5: 在 handleToolUseEvent 增量触发 Start/Delta**

把 `handleToolUseEvent` 里写 input 的那段（~698-706）：

```go
	if current != nil {
		if input, ok := event["input"].(string); ok {
			current.InputBuffer.WriteString(input)
		} else if inputObj, ok := event["input"].(map[string]interface{}); ok {
			data, _ := json.Marshal(inputObj)
			current.InputBuffer.Reset()
			current.InputBuffer.Write(data)
		}
	}
```

改为：

```go
	if current != nil {
		startToolUseIfNeeded(current, callback)
		if input, ok := event["input"].(string); ok {
			current.InputBuffer.WriteString(input)
			// 仅在 Start 已触发后才发增量 delta。GeneratedID 工具的 Start 推迟到
			// finishToolUse（避免临时 ID 与最终真实 ID 不一致），此时不发增量、也不置
			// EmittedDelta，留待兜底补发完整 partial_json，保证参数不丢且顺序正确。
			if input != "" && callback != nil && callback.OnToolUseDelta != nil && current.Started {
				callback.OnToolUseDelta(current.ToolUseID, input)
				current.EmittedDelta = true
			}
		} else if inputObj, ok := event["input"].(map[string]interface{}); ok {
			data, _ := json.Marshal(inputObj)
			current.InputBuffer.Reset()
			current.InputBuffer.Write(data)
			// map 快照无法安全逐片转发，留待 finishToolUse 兜底补发完整 delta
		}
	}
```

> **评审修复（commit `b60e0f1`）**：delta 触发条件里的 `&& current.Started` 是评审阶段补上的。
> 早期实现缺这个守卫时，GeneratedID 工具（仅 name、无显式 toolUseId）的 delta 会先于
> start 触发，导致 Claude/Responses handler 因块未建立而丢弃 delta、参数丢失。加守卫后，
> 未 Start 的工具不发增量，由 finishToolUse 兜底补发完整 partial_json。

- [ ] **Step 6: 新增 startToolUseIfNeeded 辅助函数**

在 `handleToolUseEvent` 之后、`finishToolUse` 之前插入：

```go
// startToolUseIfNeeded 在工具具备稳定 ID 后触发一次 OnToolUseStart。
// 对 GeneratedID（上游先给 name 后给真实 ID）的工具不在此触发，
// 留待 finishToolUse 兜底，避免 start 用到的临时 ID 与最终 ID 不一致。
func startToolUseIfNeeded(state *toolUseState, callback *KiroStreamCallback) {
	if state == nil || state.Started || state.Name == "" || state.GeneratedID {
		return
	}
	if callback == nil || callback.OnToolUseStart == nil {
		return
	}
	if state.ToolUseID == "" {
		return
	}
	callback.OnToolUseStart(state.ToolUseID, state.Name)
	state.Started = true
}
```

- [ ] **Step 7: finishToolUse 加增量兜底**

把 `finishToolUse`（~716）整个替换为：

```go
func finishToolUse(state *toolUseState, callback *KiroStreamCallback) {
	if state == nil || state.Name == "" || callback == nil {
		return
	}
	if state.ToolUseID == "" {
		state.ToolUseID = "toolu_" + uuid.New().String()
	}
	var input map[string]interface{}
	if state.InputBuffer.Len() > 0 {
		json.Unmarshal([]byte(state.InputBuffer.String()), &input)
	}
	if input == nil {
		input = make(map[string]interface{})
	}

	// 增量模式兜底：若启用了增量回调但尚未 Start（如 GeneratedID 或 map 快照），补 Start。
	if callback.OnToolUseStart != nil && !state.Started {
		callback.OnToolUseStart(state.ToolUseID, state.Name)
		state.Started = true
	}
	// 若启用了增量回调但从未发过 delta，补发一个完整 partial_json，保证客户端拿到完整参数。
	if callback.OnToolUseDelta != nil && !state.EmittedDelta && state.InputBuffer.Len() > 0 {
		callback.OnToolUseDelta(state.ToolUseID, state.InputBuffer.String())
		state.EmittedDelta = true
	}

	if callback.OnToolUse != nil {
		callback.OnToolUse(KiroToolUse{
			ToolUseID: state.ToolUseID,
			Name:      state.Name,
			Input:     input,
		})
	}
}
```

- [ ] **Step 8: 运行新测试 + 回归测试**

Run: `go test ./proxy/ -run "TestHandleToolUseEvent|TestParseEventStream" -v`
Expected: 全部 PASS（含新增 2 个与现有 4 个）

- [ ] **Step 9: 提交**

```bash
git add proxy/kiro.go proxy/kiro_test.go
git commit -m "feat: add incremental tool-use callbacks to stream parser"
```

---

## Task 2: CallKiroAPI 扩展名称还原（kiro.go）

**Files:**
- Modify: `proxy/kiro.go`（`CallKiroAPI` 的 wrap 段 ~310-322）

**背景:** `CallKiroAPI` 现在只 wrap `OnToolUse` 来用 `nameMap` 还原工具原名。新增的 `OnToolUseStart` 也带 name 参数，同样需要还原。`OnToolUseDelta` 只含 ID + partial_json，无需还原。

- [ ] **Step 1: 扩展 wrap 逻辑**

把 `proxy/kiro.go` 的这段（~310-322）：

```go
	// Wrap OnToolUse to restore original tool names for the client.
	if callback != nil && callback.OnToolUse != nil && len(payload.ToolNameMap) > 0 {
		originalOnToolUse := callback.OnToolUse
		nameMap := payload.ToolNameMap
		wrapped := *callback
		wrapped.OnToolUse = func(tu KiroToolUse) {
			if original, ok := nameMap[tu.Name]; ok {
				tu.Name = original
			}
			originalOnToolUse(tu)
		}
		callback = &wrapped
	}
```

替换为：

```go
	// Wrap tool-use callbacks to restore original tool names for the client.
	if callback != nil && len(payload.ToolNameMap) > 0 &&
		(callback.OnToolUse != nil || callback.OnToolUseStart != nil) {
		nameMap := payload.ToolNameMap
		wrapped := *callback
		if callback.OnToolUse != nil {
			originalOnToolUse := callback.OnToolUse
			wrapped.OnToolUse = func(tu KiroToolUse) {
				if original, ok := nameMap[tu.Name]; ok {
					tu.Name = original
				}
				originalOnToolUse(tu)
			}
		}
		if callback.OnToolUseStart != nil {
			originalOnToolUseStart := callback.OnToolUseStart
			wrapped.OnToolUseStart = func(id, name string) {
				if original, ok := nameMap[name]; ok {
					name = original
				}
				originalOnToolUseStart(id, name)
			}
		}
		callback = &wrapped
	}
```

- [ ] **Step 2: 编译**

Run: `go build ./...`
Expected: 成功

- [ ] **Step 3: 提交**

```bash
git add proxy/kiro.go
git commit -m "feat: restore original tool name in OnToolUseStart wrap"
```

---

## Task 3: Claude 流式 handler 拆分（handler.go）

**Files:**
- Modify: `proxy/handler.go`（`handleClaudeStream` 的 callback，OnToolUse ~1038-1077）

**背景:** 当前 `OnToolUse` 一次性做了：flush 文本、ensureMessageStart、closeActiveBlock、发 content_block_start、发单个 input_json_delta、发 content_block_stop、记账。要拆成 Start/Delta/Stop 三段。需要在 callback 外层声明一个变量记录当前工具块的 index。

- [ ] **Step 1: 在 callback 定义前声明工具块 index 变量**

在 `handleClaudeStream` 里 `splitter := &streamTagSplitter{...}` 之后、`callback := &KiroStreamCallback{` 之前，加：

```go
		toolBlockIndex := -1
```

- [ ] **Step 2: 用三段回调替换 OnToolUse**

把当前的 `OnToolUse: func(tu KiroToolUse) { ... }`（~1038-1077，整段）替换为：

```go
			OnToolUseStart: func(toolUseID, name string) {
				splitter.flush()
				ensureMessageStart()
				closeActiveBlock()
				toolBlockIndex = nextContentIndex
				nextContentIndex++
				h.sendSSE(w, flusher, "content_block_start", map[string]interface{}{
					"type":  "content_block_start",
					"index": toolBlockIndex,
					"content_block": map[string]interface{}{
						"type":  "tool_use",
						"id":    toolUseID,
						"name":  name,
						"input": map[string]interface{}{},
					},
				})
			},
			OnToolUseDelta: func(toolUseID, partialJSON string) {
				if toolBlockIndex < 0 {
					return
				}
				h.sendSSE(w, flusher, "content_block_delta", map[string]interface{}{
					"type":  "content_block_delta",
					"index": toolBlockIndex,
					"delta": map[string]interface{}{
						"type":         "input_json_delta",
						"partial_json": partialJSON,
					},
				})
			},
			OnToolUse: func(tu KiroToolUse) {
				rawContentBuilder.WriteString(tu.Name)
				if b, err := json.Marshal(tu.Input); err == nil {
					rawContentBuilder.Write(b)
				}
				toolUses = append(toolUses, tu)
				if toolBlockIndex >= 0 {
					h.sendSSE(w, flusher, "content_block_stop", map[string]interface{}{
						"type":  "content_block_stop",
						"index": toolBlockIndex,
					})
					toolBlockIndex = -1
				}
			},
```

> 说明：原逻辑里 content_block_start 的 input 是空对象 `{}`，参数全靠 input_json_delta 累加，这与拆分后一致。原 `closeActiveBlock()` + `ensureMessageStart()` 移入 Start。`rawContentBuilder`/`toolUses` 记账移入最终 OnToolUse。

- [ ] **Step 3: 编译**

Run: `go build ./...`
Expected: 成功

- [ ] **Step 4: 跑 Claude 测试**

Run: `go test ./proxy/ -run TestClaude -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add proxy/handler.go
git commit -m "feat: stream tool-use incrementally in handleClaudeStream"
```

---

## Task 4: OpenAI 流式 handler 拆分（handler.go）

**Files:**
- Modify: `proxy/handler.go`（`handleOpenAIStream` 的 callback，OnToolUse ~1557-1594）

**背景:** 当前 `OnToolUse` 一次性发一个含完整 arguments 的 tool_calls chunk。拆分为：Start 发带 name + 空 arguments 的首块，Delta 发 arguments 增量块，最终 OnToolUse 记账并 `toolCallIndex++`。OpenAI delta 的 `index` 用当前 `toolCallIndex`（在最终 OnToolUse 才自增）。

- [ ] **Step 1: 用三段回调替换 OnToolUse**

把当前的 `OnToolUse: func(tu KiroToolUse) { ... }`（~1557-1594，整段，从 `splitter.flush()` 到 `responseStarted = true`）替换为：

```go
			OnToolUseStart: func(toolUseID, name string) {
				splitter.flush()
				chunk := map[string]interface{}{
					"id":      chatID,
					"object":  "chat.completion.chunk",
					"created": time.Now().Unix(),
					"model":   model,
					"choices": []map[string]interface{}{{
						"index": 0,
						"delta": map[string]interface{}{
							"tool_calls": []map[string]interface{}{{
								"index": toolCallIndex,
								"id":    toolUseID,
								"type":  "function",
								"function": map[string]string{
									"name":      name,
									"arguments": "",
								},
							}},
						},
						"finish_reason": nil,
					}},
				}
				data, _ := json.Marshal(chunk)
				fmt.Fprintf(w, "data: %s\n\n", string(data))
				flusher.Flush()
				responseStarted = true
			},
			OnToolUseDelta: func(toolUseID, partialJSON string) {
				chunk := map[string]interface{}{
					"id":      chatID,
					"object":  "chat.completion.chunk",
					"created": time.Now().Unix(),
					"model":   model,
					"choices": []map[string]interface{}{{
						"index": 0,
						"delta": map[string]interface{}{
							"tool_calls": []map[string]interface{}{{
								"index": toolCallIndex,
								"function": map[string]string{
									"arguments": partialJSON,
								},
							}},
						},
						"finish_reason": nil,
					}},
				}
				data, _ := json.Marshal(chunk)
				fmt.Fprintf(w, "data: %s\n\n", string(data))
				flusher.Flush()
			},
			OnToolUse: func(tu KiroToolUse) {
				args, _ := json.Marshal(tu.Input)
				rawContentBuilder.WriteString(tu.Name)
				rawContentBuilder.Write(args)
				tc := ToolCall{ID: tu.ToolUseID, Type: "function"}
				tc.Function.Name = tu.Name
				tc.Function.Arguments = string(args)
				toolCalls = append(toolCalls, tc)
				toolCallIndex++
			},
```

> 说明：原逻辑用 `string(args)`（完整参数）一次发出；拆分后 arguments 由多个 delta 累加。最终 OnToolUse 的 `toolCalls` 记账用完整 `args`（用于非流式统计与历史），与流式发送相互独立、不冲突。

- [ ] **Step 2: 编译**

Run: `go build ./...`
Expected: 成功

- [ ] **Step 3: 跑 OpenAI 测试**

Run: `go test ./proxy/ -run "TestOpenAI|TestValidateOpenAI" -v`
Expected: PASS

- [ ] **Step 4: 提交**

```bash
git add proxy/handler.go
git commit -m "feat: stream tool-use incrementally in handleOpenAIStream"
```

---

## Task 5: Responses 流式 handler 拆分（responses_handler.go）

**Files:**
- Modify: `proxy/responses_handler.go`（`handleResponsesStream` 的 callback，OnToolUse ~397-462）

**背景:** 当前 `OnToolUse` 一次性做：若 message 块开着先收尾、发 output_item.added(function_call)、发完整 function_call_arguments.delta、发 output_item.done。拆成 Start（收尾 message + added）、Delta（arguments 增量）、最终 OnToolUse（done）。需要在 callback 外层声明记录当前 function_call 的 ID。

- [ ] **Step 1: 在 callback 定义前声明变量**

在 `handleResponsesStream` 里 `callback := &KiroStreamCallback{` 之前，加：

```go
		curFcID := ""
```

- [ ] **Step 2: 用三段回调替换 OnToolUse**

把当前的 `OnToolUse: func(tu KiroToolUse) { ... }`（~397-462，整段）替换为：

```go
			OnToolUseStart: func(toolUseID, name string) {
				if messageStarted {
					send("response.content_part.done", map[string]interface{}{
						"type":          "response.content_part.done",
						"item_id":       messageItemID,
						"output_index":  outputIndex,
						"content_index": contentIndex,
						"part": map[string]interface{}{
							"type": "output_text",
							"text": fullText.String(),
						},
					})
					send("response.output_item.done", map[string]interface{}{
						"type":         "response.output_item.done",
						"output_index": outputIndex,
						"item": map[string]interface{}{
							"id":     messageItemID,
							"type":   "message",
							"role":   "assistant",
							"status": "completed",
							"content": []map[string]interface{}{{
								"type": "output_text",
								"text": fullText.String(),
							}},
						},
					})
					messageStarted = false
					outputIndex++
				}
				curFcID = generateOutputItemID("fc")
				send("response.output_item.added", map[string]interface{}{
					"type":         "response.output_item.added",
					"output_index": outputIndex,
					"item": map[string]interface{}{
						"id":        curFcID,
						"type":      "function_call",
						"status":    "in_progress",
						"call_id":   toolUseID,
						"name":      name,
						"arguments": "",
					},
				})
				responseStarted = true
			},
			OnToolUseDelta: func(toolUseID, partialJSON string) {
				if curFcID == "" {
					return
				}
				send("response.function_call_arguments.delta", map[string]interface{}{
					"type":         "response.function_call_arguments.delta",
					"item_id":      curFcID,
					"output_index": outputIndex,
					"delta":        partialJSON,
				})
			},
			OnToolUse: func(tu KiroToolUse) {
				toolUses = append(toolUses, tu)
				args, _ := json.Marshal(tu.Input)
				send("response.output_item.done", map[string]interface{}{
					"type":         "response.output_item.done",
					"output_index": outputIndex,
					"item": map[string]interface{}{
						"id":        curFcID,
						"type":      "function_call",
						"status":    "completed",
						"call_id":   tu.ToolUseID,
						"name":      tu.Name,
						"arguments": string(args),
					},
				})
				outputIndex++
				curFcID = ""
			},
```

> 说明：原逻辑 added 的 arguments 为 ""、delta 发完整 args、done 发完整 args。拆分后 added 仍是 ""，delta 改为增量累加，done 仍带完整 args（OpenAI Responses 协议 done 带最终值是规范的）。

- [ ] **Step 3: 编译**

Run: `go build ./...`
Expected: 成功

- [ ] **Step 4: 跑 Responses 测试**

Run: `go test ./proxy/ -run TestResponses -v`
Expected: PASS

- [ ] **Step 5: 提交**

```bash
git add proxy/responses_handler.go
git commit -m "feat: stream tool-use incrementally in handleResponsesStream"
```

---

## Task 6: 全量验证

**Files:** 无（仅验证）

- [ ] **Step 1: 全量编译**

Run: `go build ./...`
Expected: 成功，无错误。

- [ ] **Step 2: 全量测试**

Run: `go test ./...`
Expected: 全部 PASS。

- [ ] **Step 3: vet 检查**

Run: `go vet ./...`
Expected: 无新增告警。

---

## Task 7: 评审修复 — 多工具测试与 Start/Delta 顺序守卫（实际补充）

**背景:** 评审阶段补写 spec 计划里要求的多工具测试时，发现一个真实 bug：GeneratedID 工具（仅 name、无显式 toolUseId）的 `OnToolUseDelta` 会先于 `OnToolUseStart` 触发，使 Claude/Responses handler 因块未建立而丢弃 delta、工具参数丢失。

**Files:**
- Modify: `proxy/kiro.go`（`handleToolUseEvent` 的 string delta 分支）
- Test: `proxy/kiro_test.go`

- [ ] **Step 1: 补多工具与顺序测试（其中一个会暴露 bug）**

向 `proxy/kiro_test.go` 追加三个测试：
- `TestHandleToolUseEventMultipleToolsWithExplicitIDsStreamEach`：两个带显式 ID 的工具，断言完整序列 `A start/delta/delta/use, B start/delta/delta/use`。
- `TestHandleToolUseEventMultipleToolsByNameSwitchStreamEach`：name 切换的两个工具，断言第二个工具 start 先于 use。
- `TestHandleToolUseEventStartPrecedesDeltaForGeneratedID`：GeneratedID 工具首个回调必须是 start 而非 delta。

- [ ] **Step 2: 运行确认 bug 暴露**

Run: `go test ./proxy/ -run TestHandleToolUseEventStartPrecedesDeltaForGeneratedID -v`
Expected: FAIL，实际回调顺序为 `[delta, delta, start]`

- [ ] **Step 3: 加 state.Started 守卫修复**

`handleToolUseEvent` 的 string delta 分支，触发条件加 `&& current.Started`（见 Task 1 Step 5 已更新的最终代码）。

- [ ] **Step 4: 运行确认修复**

Run: `go test ./proxy/ -run "TestHandleToolUseEvent|TestParseEventStream" -v`
Expected: 全部 PASS（含 3 个新增多工具/顺序测试）

- [ ] **Step 5: 全量验证 + 提交**

Run: `go build ./... ; go test ./... ; go vet ./...`
Expected: 全绿

```bash
git add proxy/kiro.go proxy/kiro_test.go
git commit -m "fix: emit tool-use delta only after start to avoid dropped args for generated-ID tools"
```

---

## 自检

- **Spec 覆盖**：回调结构改造（Task 1）、handleToolUseEvent 增量+map 兜底（Task 1）、CallKiroAPI 名称还原（Task 2）、Claude/OpenAI/Responses 三个 handler 拆分（Task 3/4/5）、向后兼容（Task 1 测试 + 现有测试）、全量验证（Task 6）。全部覆盖。
- **占位符**：无 TBD/TODO，所有代码步骤给出完整代码。
- **类型一致性**：`OnToolUseStart func(toolUseID, name string)`、`OnToolUseDelta func(toolUseID, partialJSON string)` 在 Task 1 定义，Task 2/3/4/5 一致使用。`toolUseState` 的 `Started`/`EmittedDelta` 在 Task 1 定义并被 `startToolUseIfNeeded`/`finishToolUse` 使用。handler 局部变量 `toolBlockIndex`（Claude）、`toolCallIndex`（OpenAI 沿用现有）、`curFcID`（Responses）名称一致。
