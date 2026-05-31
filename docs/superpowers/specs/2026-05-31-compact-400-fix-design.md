# Compact 请求 AmazonQ 400 修复 — 设计文档

日期: 2026-05-31

## 问题

Claude Code 发送 compact（anchored summary）请求到 Kiro-Go 时，AmazonQ 返回 HTTP 400：

```json
{"message": "Improperly formed request.", "reason": null}
```

普通 agentic 对话不受影响，仅 compact 操作触发。

### 根因（已通过控制变量实验确认）

**Kiro/AmazonQ 要求：如果 history 中存在 `toolUses`/`toolResults` 结构，请求必须在 `tools` 字段声明对应的工具定义。**

Compact 请求的特殊性：
- `tools: []`（空，因为 compact 不需要调用工具）
- `messages` 包含完整对话历史（含 201 个 tool_use/tool_result 回合）
- 最后一条 user 消息是 `tool_result + text` 混合（工具输出 + compact 指令）

普通 agentic 对话不受影响，因为它们**总是带着完整的 tools 声明**。

### 实验验证过程

通过 11 轮控制变量实验（共 20+ 个测试用例）逐步隔离：

| 实验 | 结论 |
|------|------|
| 原始请求 + 声明所有工具 | 200 OK |
| 合成 201 条消息工具对话 | 200 OK |
| 真实消息 4 条（含工具结构）无工具声明 | 400 |
| 真实消息 276 条（清除工具结构）| 200 OK |
| 最小复现：history 末尾 assistant 有 tool_use + current 无 tool_result | 400 |
| 最小复现：history 末尾 assistant 有 tool_use + current 有 tool_result | 200 OK |

关键发现：不是消息数量、不是 payload 大小、不是 tool_result+text 混合本身——**唯一触发条件是 history 有工具结构但请求未声明 tools**。

### 次要问题

对话以 assistant 消息开头时（compact 历史片段的常见情况），`trimLeadingAssistantHistory` 移除 leading assistant 后，紧随其后的 user(tool_result-only) 消息成为孤立的 tool_result（前面没有对应的 assistant tool_use），也会触发 400。

## 方案

### 1. `collectHistoryTools` — 自动声明工具

当 `req.Tools` 为空但 history 中有 `toolUses` 时，自动从 history 收集工具名并生成最小化的工具声明。

```go
func collectHistoryTools(history []KiroHistoryMessage) ([]KiroToolWrapper, map[string]string) {
    // 扫描 history 中所有 assistant 的 ToolUses，收集唯一工具名
    // 对每个工具名做 sanitizeToolName + shortenToolName（与 convertClaudeTools 一致）
    // 生成最小 tool definition: name + "Tool: {name}" description + empty object schema
    // 返回 tools 和 nameMap（仅在名称实际变化时填充）
}
```

调用点（`ClaudeToKiro` 中）：

```go
kiroTools, toolNameMap := convertClaudeTools(req.Tools)

// If no tools declared but history contains tool_use, auto-declare them.
if len(kiroTools) == 0 {
    kiroTools, toolNameMap = collectHistoryTools(history)
}
```

### 2. `trimLeadingAssistantHistory` 增强

移除 leading assistant 后，继续跳过紧随其后的孤立 user(tool_result-only) 消息：

```go
func trimLeadingAssistantHistory(history []KiroHistoryMessage) []KiroHistoryMessage {
    idx := 0
    for idx < len(history) {
        msg := history[idx]
        if msg.AssistantResponseMessage != nil {
            idx++; continue  // 移除 leading assistant
        }
        if msg.UserInputMessage != nil {
            hasText := strings.TrimSpace(msg.UserInputMessage.Content) != ""
            hasImages := len(msg.UserInputMessage.Images) > 0
            hasToolResults := ctx != nil && len(ctx.ToolResults) > 0
            if !hasText && !hasImages && hasToolResults {
                idx++; continue  // 移除孤立 tool_result
            }
        }
        break  // 遇到有实际内容的 user 消息，停止
    }
    // ...
}
```

## 影响范围

| 场景 | 影响 |
|------|------|
| 普通 agentic 对话（tools 非空）| **零影响** — `len(kiroTools) == 0` 为 false，`collectHistoryTools` 不会被调用 |
| Compact 请求（tools 为空）| 自动声明工具，修复 400 |
| 纯文本对话（无工具历史）| **零影响** — `collectHistoryTools` 返回 nil |
| 对话以 user text 开头 | **零影响** — trim 循环第一次就 break |
| OpenAI 兼容路径 | 未加保护（风险极低：compact 是 Claude Code 专有行为）|

## 设计决策

1. **不做全局历史工具扁平化**：实验证明 Kiro 完全接受历史中的结构化工具数据，只要请求声明了 tools。扁平化会损失信息。
2. **工具名做 sanitize**：与 `convertClaudeTools` 保持一致的 camelCase 转换和长度截断，防御超长 MCP 工具名。
3. **nameMap 仅在名称变化时填充**：避免安装无意义的 no-op 回调 wrapper。
4. **不基于 system prompt 关键词判断 compact**：用 `len(req.Tools) == 0` 作为触发条件更稳健，也覆盖了其他可能的"无工具但有历史工具记录"的边缘情况。

## 验证

- 原始 compact 请求：200 OK
- 输出内容：正确的 anchored summary（4920 字符，包含 Goal/Progress/Key Decisions/Next Steps/Critical Context/Relevant Files 全部 section）
- 所有既有测试通过
- 6 个新增测试覆盖关键场景
