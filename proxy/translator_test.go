package proxy

import (
	"strings"
	"testing"
)

func TestExtractOpenAIMessageTextStructured(t *testing.T) {
	content := []interface{}{
		map[string]interface{}{"type": "text", "text": "alpha"},
		map[string]interface{}{"type": "input_text", "text": "beta"},
	}

	if got := extractOpenAIMessageText(content); got != "alphabeta" {
		t.Fatalf("expected concatenated structured text, got %q", got)
	}

	nested := map[string]interface{}{
		"content": []interface{}{map[string]interface{}{"type": "text", "text": "nested"}},
	}
	if got := extractOpenAIMessageText(nested); got != "nested" {
		t.Fatalf("expected nested content extraction, got %q", got)
	}
}

func TestOpenAIToKiroPreservesStructuredAssistantAndToolContent(t *testing.T) {
	req := &OpenAIRequest{
		Model: "claude-sonnet-4.5",
		Messages: []OpenAIMessage{
			{
				Role: "system",
				Content: []interface{}{
					map[string]interface{}{"type": "text", "text": "system-a"},
					map[string]interface{}{"type": "text", "text": "system-b"},
				},
			},
			{Role: "user", Content: "first-question"},
			{
				Role: "assistant",
				Content: []interface{}{
					map[string]interface{}{"type": "text", "text": "assistant-structured"},
				},
			},
			{
				Role:       "tool",
				ToolCallID: "call_1",
				Content: []interface{}{
					map[string]interface{}{"type": "text", "text": "tool-result-structured"},
				},
			},
		},
	}

	payload := OpenAIToKiro(req, false)

	// History starts with a priming pair.
	if len(payload.ConversationState.History) != 4 {
		t.Fatalf("expected 4 history items (2 priming + 2 conversation), got %d", len(payload.ConversationState.History))
	}

	// history[0]: priming user
	primingUser := payload.ConversationState.History[0].UserInputMessage
	if primingUser == nil {
		t.Fatalf("expected history[0] to be priming user message")
	}
	if !strings.Contains(primingUser.Content, "system-a") || !strings.Contains(primingUser.Content, "system-b") {
		t.Fatalf("expected priming user message to contain system prompt, got %q", primingUser.Content)
	}
	if strings.Contains(primingUser.Content, "first-question") {
		t.Fatalf("expected system prompt priming not to contain user question, got %q", primingUser.Content)
	}

	// history[1]: priming assistant
	primingAssistant := payload.ConversationState.History[1].AssistantResponseMessage
	if primingAssistant == nil {
		t.Fatalf("expected history[1] to be priming assistant message")
	}
	if primingAssistant.Content != "I will follow these instructions." {
		t.Fatalf("expected priming assistant ack, got %q", primingAssistant.Content)
	}

	// history[2]: first user turn
	firstConvUser := payload.ConversationState.History[2].UserInputMessage
	if firstConvUser == nil {
		t.Fatalf("expected history[2] to be first conversation user message")
	}
	if !strings.Contains(firstConvUser.Content, "first-question") {
		t.Fatalf("expected history[2] to contain first-question, got %q", firstConvUser.Content)
	}

	// history[3]: assistant reply
	historyAssistant := payload.ConversationState.History[3].AssistantResponseMessage
	if historyAssistant == nil {
		t.Fatalf("expected history[3] to be assistant message")
	}
	if historyAssistant.Content != "assistant-structured" {
		t.Fatalf("expected assistant structured content to be preserved, got %q", historyAssistant.Content)
	}

	cur := payload.ConversationState.CurrentMessage.UserInputMessage
	if !strings.Contains(cur.Content, "tool-result-structured") {
		t.Fatalf("expected tool-result continuation content, got %q", cur.Content)
	}
	if cur.UserInputMessageContext == nil || len(cur.UserInputMessageContext.ToolResults) != 1 {
		t.Fatalf("expected one tool result in current context")
	}
	gotToolText := cur.UserInputMessageContext.ToolResults[0].Content[0].Text
	if gotToolText != "tool-result-structured" {
		t.Fatalf("expected structured tool result text, got %q", gotToolText)
	}
}

func TestOpenAIToKiroAssistantMapContentInHistory(t *testing.T) {
	req := &OpenAIRequest{
		Model: "claude-sonnet-4.5",
		Messages: []OpenAIMessage{
			{Role: "user", Content: "u1"},
			{Role: "assistant", Content: map[string]interface{}{"type": "text", "text": "assistant-map"}},
			{Role: "user", Content: "u2"},
		},
	}

	payload := OpenAIToKiro(req, false)

	if len(payload.ConversationState.History) != 2 {
		t.Fatalf("expected 2 history entries, got %d", len(payload.ConversationState.History))
	}
	assistant := payload.ConversationState.History[1].AssistantResponseMessage
	if assistant == nil {
		t.Fatalf("expected second history entry to be assistant")
	}
	if assistant.Content != "assistant-map" {
		t.Fatalf("expected assistant map content preserved, got %q", assistant.Content)
	}
}

func TestOpenAIToKiroAssistantToolCallsDoNotInjectPlaceholder(t *testing.T) {
	req := &OpenAIRequest{
		Model: "claude-sonnet-4.5",
		Messages: []OpenAIMessage{
			{Role: "user", Content: "find weather"},
			{
				Role:    "assistant",
				Content: nil,
				ToolCalls: []ToolCall{{
					ID:   "call_1",
					Type: "function",
					Function: struct {
						Name      string `json:"name"`
						Arguments string `json:"arguments"`
					}{Name: "get_weather", Arguments: "{}"},
				}},
			},
			{Role: "user", Content: "continue"},
		},
	}

	payload := OpenAIToKiro(req, false)
	if len(payload.ConversationState.History) < 2 {
		t.Fatalf("expected history with assistant tool call")
	}
	assistant := payload.ConversationState.History[1].AssistantResponseMessage
	if assistant == nil {
		t.Fatalf("expected assistant history entry")
	}
	if assistant.Content != "" {
		t.Fatalf("expected empty assistant content for tool-call-only turn, got %q", assistant.Content)
	}
}

func TestOpenAIConversationIDStableFromAnchor(t *testing.T) {
	baseMessages := []OpenAIMessage{
		{Role: "system", Content: "You are helpful"},
		{Role: "user", Content: "Build calculator"},
		{Role: "assistant", Content: "Sure"},
		{Role: "user", Content: "Continue"},
	}

	reqA := &OpenAIRequest{Model: "claude-sonnet-4.5", Messages: baseMessages}
	reqB := &OpenAIRequest{Model: "claude-sonnet-4.5", Messages: append(baseMessages, OpenAIMessage{Role: "assistant", Content: "Next step"})}

	payloadA := OpenAIToKiro(reqA, false)
	payloadB := OpenAIToKiro(reqB, false)

	if payloadA.ConversationState.ConversationID == "" || payloadB.ConversationState.ConversationID == "" {
		t.Fatalf("expected non-empty conversation IDs")
	}
	if payloadA.ConversationState.ConversationID != payloadB.ConversationState.ConversationID {
		t.Fatalf("expected stable conversation ID across turns, got %q vs %q", payloadA.ConversationState.ConversationID, payloadB.ConversationState.ConversationID)
	}
}

func TestClaudeConversationIDStableFromAnchor(t *testing.T) {
	reqA := &ClaudeRequest{
		Model:  "claude-sonnet-4.5",
		System: "sys",
		Messages: []ClaudeMessage{
			{Role: "user", Content: "hello"},
		},
	}
	reqB := &ClaudeRequest{
		Model:  "claude-sonnet-4.5",
		System: "sys",
		Messages: []ClaudeMessage{
			{Role: "user", Content: "hello"},
			{Role: "assistant", Content: "ok"},
			{Role: "user", Content: "next"},
		},
	}

	payloadA := ClaudeToKiro(reqA, false)
	payloadB := ClaudeToKiro(reqB, false)

	if payloadA.ConversationState.ConversationID == "" || payloadB.ConversationState.ConversationID == "" {
		t.Fatalf("expected non-empty conversation IDs")
	}
	if payloadA.ConversationState.ConversationID != payloadB.ConversationState.ConversationID {
		t.Fatalf("expected stable conversation ID across turns, got %q vs %q", payloadA.ConversationState.ConversationID, payloadB.ConversationState.ConversationID)
	}
}

func TestOpenAIConversationIDRandomForSyntheticAnchor(t *testing.T) {
	req := &OpenAIRequest{
		Model: "claude-sonnet-4.5",
		Messages: []OpenAIMessage{
			{Role: "assistant", Content: "prefill"},
		},
	}

	payloadA := OpenAIToKiro(req, false)
	payloadB := OpenAIToKiro(req, false)

	if payloadA.ConversationState.ConversationID == payloadB.ConversationState.ConversationID {
		t.Fatalf("expected synthetic anchor to generate non-deterministic conversation IDs")
	}
}

func TestClaudeToKiroDropsLeadingAssistantHistory(t *testing.T) {
	req := &ClaudeRequest{
		Model: "claude-sonnet-4.5",
		Messages: []ClaudeMessage{
			{Role: "assistant", Content: "prefill"},
			{Role: "user", Content: "real user message"},
		},
	}

	payload := ClaudeToKiro(req, false)

	if len(payload.ConversationState.History) != 0 {
		t.Fatalf("expected leading assistant-only history to be dropped, got %d entries", len(payload.ConversationState.History))
	}

	if strings.Contains(payload.ConversationState.CurrentMessage.UserInputMessage.Content, "Begin conversation") {
		t.Fatalf("unexpected synthetic Begin conversation injection in current content: %q", payload.ConversationState.CurrentMessage.UserInputMessage.Content)
	}
}

func TestKiroToClaudeResponseCanEmitEmptyThinkingBlock(t *testing.T) {
	resp := KiroToClaudeResponse("final answer", "", true, nil, 10, 20, "claude-sonnet-4.6")

	if len(resp.Content) != 2 {
		t.Fatalf("expected empty thinking block plus text block, got %d blocks", len(resp.Content))
	}
	if resp.Content[0].Type != "thinking" {
		t.Fatalf("expected first block to be thinking, got %#v", resp.Content[0])
	}
	if resp.Content[0].Thinking != "" {
		t.Fatalf("expected omitted thinking block to have empty content, got %#v", resp.Content[0].Thinking)
	}
	if resp.Content[1].Type != "text" || resp.Content[1].Text != "final answer" {
		t.Fatalf("expected text block to be preserved, got %#v", resp.Content[1])
	}
}

func TestToolResultsContinuationIncludesInstructionPrefix(t *testing.T) {
	req := &OpenAIRequest{
		Model: "claude-sonnet-4.5",
		Messages: []OpenAIMessage{
			{Role: "user", Content: "find data"},
			{Role: "assistant", ToolCalls: []ToolCall{{
				ID:   "call_1",
				Type: "function",
				Function: struct {
					Name      string `json:"name"`
					Arguments string `json:"arguments"`
				}{Name: "fetch", Arguments: "{}"},
			}}},
			{Role: "tool", ToolCallID: "call_1", Content: "result-1"},
		},
	}

	payload := OpenAIToKiro(req, false)
	content := payload.ConversationState.CurrentMessage.UserInputMessage.Content

	if !strings.Contains(content, toolResultsContinuationPrefix) {
		t.Fatalf("expected tool continuation prefix, got %q", content)
	}
	if !strings.Contains(content, "result-1") {
		t.Fatalf("expected tool result text in continuation content, got %q", content)
	}
}

func TestEnsureObjectSchemaRemovesKiroRejectedFieldsRecursively(t *testing.T) {
	input := map[string]interface{}{
		"type":                 "object",
		"required":             []interface{}{},
		"additionalProperties": false,
		"properties": map[string]interface{}{
			"path": map[string]interface{}{
				"type":                 "string",
				"required":             nil,
				"additionalProperties": map[string]interface{}{"type": "string"},
			},
			"options": map[string]interface{}{
				"type":                 "object",
				"additionalProperties": false,
				"properties": map[string]interface{}{
					"force": map[string]interface{}{"type": "boolean"},
				},
			},
		},
		"anyOf": []interface{}{
			map[string]interface{}{
				"type":                 "object",
				"required":             []interface{}{},
				"additionalProperties": false,
			},
		},
	}

	got := ensureObjectSchema(input).(map[string]interface{})
	if schemaContainsKey(got, "additionalProperties") {
		t.Fatalf("expected additionalProperties to be removed recursively, got %#v", got)
	}
	if schemaContainsKey(got, "required") {
		t.Fatalf("expected empty/nil required fields to be removed recursively, got %#v", got)
	}
	if _, stillPresent := input["additionalProperties"]; !stillPresent {
		t.Fatalf("expected sanitizer not to mutate caller schema")
	}
}

func TestConvertOpenAIToolsSanitizesSchemaAndDescription(t *testing.T) {
	var tool OpenAITool
	tool.Type = "function"
	tool.Function.Name = "read_file"
	tool.Function.Parameters = map[string]interface{}{
		"type":                 "object",
		"required":             []string{},
		"additionalProperties": false,
	}

	tools := convertOpenAITools([]OpenAITool{tool})
	if len(tools) != 1 {
		t.Fatalf("expected one converted tool, got %d", len(tools))
	}
	if strings.TrimSpace(tools[0].ToolSpecification.Description) == "" {
		t.Fatalf("expected fallback tool description")
	}
	schema := tools[0].ToolSpecification.InputSchema.JSON.(map[string]interface{})
	if schemaContainsKey(schema, "additionalProperties") {
		t.Fatalf("expected OpenAI tool schema to be sanitized, got %#v", schema)
	}
	if schemaContainsKey(schema, "required") {
		t.Fatalf("expected empty required field to be removed, got %#v", schema)
	}
}

func schemaContainsKey(value interface{}, key string) bool {
	switch v := value.(type) {
	case map[string]interface{}:
		if _, ok := v[key]; ok {
			return true
		}
		for _, child := range v {
			if schemaContainsKey(child, key) {
				return true
			}
		}
	case []interface{}:
		for _, child := range v {
			if schemaContainsKey(child, key) {
				return true
			}
		}
	}
	return false
}

func TestParseModelAndThinking(t *testing.T) {
	tests := []struct {
		name         string
		input        string
		wantModel    string
		wantThinking bool
	}{
		// Format normalization: dash → dot for new versions without code changes.
		{"new opus dash form", "claude-opus-4-8", "claude-opus-4.8", false},
		{"new opus dot form", "claude-opus-4.8", "claude-opus-4.8", false},
		{"existing opus dash form", "claude-opus-4-7", "claude-opus-4.7", false},
		{"existing opus dot form", "claude-opus-4.7", "claude-opus-4.7", false},
		{"sonnet dash form", "claude-sonnet-4-6", "claude-sonnet-4.6", false},
		{"sonnet dot form", "claude-sonnet-4.6", "claude-sonnet-4.6", false},
		{"haiku dash form", "claude-haiku-4-5", "claude-haiku-4.5", false},
		{"haiku dot form", "claude-haiku-4.5", "claude-haiku-4.5", false},
		{"future major bump", "claude-sonnet-5-0", "claude-sonnet-5.0", false},

		// Bare family name passes through (no minor to normalize).
		{"bare sonnet 4", "claude-sonnet-4", "claude-sonnet-4", false},

		// Dated snapshot must hit the alias before the regex rewrites it.
		{"dated sonnet snapshot", "claude-sonnet-4-20250514", "claude-sonnet-4", false},

		// Cross-family legacy IDs.
		{"claude 3.5 sonnet", "claude-3-5-sonnet", "claude-sonnet-4.5", false},
		{"claude 3 opus", "claude-3-opus", "claude-sonnet-4.5", false},
		{"claude 3 sonnet", "claude-3-sonnet", "claude-sonnet-4", false},
		{"claude 3 haiku", "claude-3-haiku", "claude-haiku-4.5", false},

		// Non-Anthropic fallbacks.
		{"gpt-4-turbo", "gpt-4-turbo", "claude-sonnet-4.5", false},
		{"gpt-4o", "gpt-4o", "claude-sonnet-4.5", false},
		{"gpt-4", "gpt-4", "claude-sonnet-4.5", false},
		{"gpt-3.5-turbo", "gpt-3.5-turbo", "claude-sonnet-4.5", false},

		// Thinking suffix is stripped before mapping.
		{"thinking suffix on dash form", "claude-opus-4-8-thinking", "claude-opus-4.8", true},
		{"thinking suffix on dot form", "claude-sonnet-4.5-thinking", "claude-sonnet-4.5", true},
		{"thinking suffix on legacy alias", "claude-3-5-sonnet-thinking", "claude-sonnet-4.5", true},

		// Unknown models pass through unchanged.
		{"unknown model", "some-other-model", "some-other-model", false},
		{"misspelled claude family", "claude-opux-4-8", "claude-opux-4-8", false},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			gotModel, gotThinking := ParseModelAndThinking(tc.input, "-thinking")
			if gotModel != tc.wantModel {
				t.Errorf("model: got %q, want %q", gotModel, tc.wantModel)
			}
			if gotThinking != tc.wantThinking {
				t.Errorf("thinking: got %v, want %v", gotThinking, tc.wantThinking)
			}
		})
	}
}

func TestTrimLeadingAssistantHistoryDropsOrphanedToolResults(t *testing.T) {
	// When conversation starts with assistant(tool_use) -> user(tool_result),
	// trimLeadingAssistantHistory should drop both, not just the assistant.
	req := &ClaudeRequest{
		Model: "claude-sonnet-4.5",
		Messages: []ClaudeMessage{
			{Role: "assistant", Content: []interface{}{
				map[string]interface{}{"type": "tool_use", "id": "call_1", "name": "grep", "input": map[string]interface{}{"pattern": "foo"}},
			}},
			{Role: "user", Content: []interface{}{
				map[string]interface{}{"type": "tool_result", "tool_use_id": "call_1", "content": "found: foo bar"},
			}},
			{Role: "assistant", Content: []interface{}{
				map[string]interface{}{"type": "tool_use", "id": "call_2", "name": "read", "input": map[string]interface{}{"filePath": "/tmp/x"}},
			}},
			{Role: "user", Content: []interface{}{
				map[string]interface{}{"type": "tool_result", "tool_use_id": "call_2", "content": "file contents"},
			}},
			{Role: "assistant", Content: "Done analyzing."},
			{Role: "user", Content: "Now summarize."},
		},
	}

	payload := ClaudeToKiro(req, false)

	// The first 4 messages (2x assistant tool_use + 2x user tool_result) should be trimmed.
	// Remaining: assistant("Done analyzing.") + user("Now summarize.") as current.
	// After system priming prepend: priming_user + priming_assistant + assistant("Done...")
	// Current: "Now summarize."
	cur := payload.ConversationState.CurrentMessage.UserInputMessage
	if !strings.Contains(cur.Content, "Now summarize") {
		t.Fatalf("expected current message to be 'Now summarize', got %q", cur.Content)
	}

	// History should NOT contain any orphaned tool_result messages
	for i, msg := range payload.ConversationState.History {
		if msg.UserInputMessage != nil && msg.UserInputMessage.UserInputMessageContext != nil {
			if len(msg.UserInputMessage.UserInputMessageContext.ToolResults) > 0 {
				t.Fatalf("history[%d] still contains structured toolResults, should have been trimmed", i)
			}
		}
	}
}

func TestTrimLeadingAssistantHistoryStopsAtUserWithText(t *testing.T) {
	// If the first user message has both text and tool_result, it should NOT be trimmed.
	req := &ClaudeRequest{
		Model: "claude-sonnet-4.5",
		Messages: []ClaudeMessage{
			{Role: "assistant", Content: []interface{}{
				map[string]interface{}{"type": "tool_use", "id": "call_1", "name": "bash", "input": map[string]interface{}{"command": "ls"}},
			}},
			{Role: "user", Content: []interface{}{
				map[string]interface{}{"type": "tool_result", "tool_use_id": "call_1", "content": "file1.txt"},
				map[string]interface{}{"type": "text", "text": "I see the file, now continue."},
			}},
			{Role: "assistant", Content: "OK."},
			{Role: "user", Content: "Summarize."},
		},
	}

	payload := ClaudeToKiro(req, false)

	// The leading assistant is trimmed, but the user with text+tool_result is kept.
	// After priming: priming_user + priming_assistant + user(text+tool_result) + assistant("OK.")
	// Current: "Summarize."
	cur := payload.ConversationState.CurrentMessage.UserInputMessage
	if !strings.Contains(cur.Content, "Summarize") {
		t.Fatalf("expected current to be 'Summarize', got %q", cur.Content)
	}

	// The user message with text should be preserved in history
	found := false
	for _, msg := range payload.ConversationState.History {
		if msg.UserInputMessage != nil && strings.Contains(msg.UserInputMessage.Content, "I see the file") {
			found = true
			break
		}
	}
	if !found {
		t.Fatalf("expected user message with text to be preserved in history")
	}
}

func TestCollectHistoryToolsAutoDeclaresWhenNoToolsProvided(t *testing.T) {
	// Simulates a compact request: tools=[] but history has tool_use structures.
	req := &ClaudeRequest{
		Model: "claude-sonnet-4.5",
		Messages: []ClaudeMessage{
			{Role: "user", Content: "Build a feature"},
			{Role: "assistant", Content: []interface{}{
				map[string]interface{}{"type": "text", "text": "I'll read the file."},
				map[string]interface{}{"type": "tool_use", "id": "call_1", "name": "read", "input": map[string]interface{}{"filePath": "/tmp/x"}},
			}},
			{Role: "user", Content: []interface{}{
				map[string]interface{}{"type": "tool_result", "tool_use_id": "call_1", "content": "file contents"},
			}},
			{Role: "assistant", Content: []interface{}{
				map[string]interface{}{"type": "text", "text": "Now editing."},
				map[string]interface{}{"type": "tool_use", "id": "call_2", "name": "apply_patch", "input": map[string]interface{}{"patchText": "..."}},
			}},
			{Role: "user", Content: []interface{}{
				map[string]interface{}{"type": "tool_result", "tool_use_id": "call_2", "content": "patched"},
			}},
			{Role: "assistant", Content: "All done."},
			{Role: "user", Content: "Update the anchored summary."},
		},
		Tools: nil, // No tools declared (compact request)
	}

	payload := ClaudeToKiro(req, false)

	// Tools should be auto-declared from history
	ctx := payload.ConversationState.CurrentMessage.UserInputMessage.UserInputMessageContext
	if ctx == nil || len(ctx.Tools) == 0 {
		t.Fatalf("expected auto-declared tools in current message context, got nil or empty")
	}

	// Should have found "read" and "apply_patch" (sanitized to "applyPatch")
	toolNames := make(map[string]bool)
	for _, tool := range ctx.Tools {
		toolNames[tool.ToolSpecification.Name] = true
	}
	// "read" stays as "read", "apply_patch" becomes "applyPatch"
	if !toolNames["read"] {
		t.Fatalf("expected 'read' in auto-declared tools, got %v", toolNames)
	}
	if !toolNames["applyPatch"] {
		t.Fatalf("expected 'applyPatch' (sanitized from apply_patch) in auto-declared tools, got %v", toolNames)
	}
}

func TestCollectHistoryToolsNotCalledWhenToolsProvided(t *testing.T) {
	// Normal agentic request: tools are provided, collectHistoryTools should NOT run.
	req := &ClaudeRequest{
		Model: "claude-sonnet-4.5",
		Messages: []ClaudeMessage{
			{Role: "user", Content: "Run ls"},
			{Role: "assistant", Content: []interface{}{
				map[string]interface{}{"type": "tool_use", "id": "call_1", "name": "bash", "input": map[string]interface{}{"command": "ls"}},
			}},
			{Role: "user", Content: []interface{}{
				map[string]interface{}{"type": "tool_result", "tool_use_id": "call_1", "content": "file1.txt"},
			}},
		},
		Tools: []ClaudeTool{
			{Name: "bash", Description: "Run a bash command", InputSchema: map[string]interface{}{"type": "object"}},
		},
	}

	payload := ClaudeToKiro(req, false)

	ctx := payload.ConversationState.CurrentMessage.UserInputMessage.UserInputMessageContext
	if ctx == nil || len(ctx.Tools) == 0 {
		t.Fatalf("expected tools in context")
	}

	// Should only have the explicitly declared "bash" tool
	if len(ctx.Tools) != 1 {
		t.Fatalf("expected exactly 1 tool (from req.Tools), got %d", len(ctx.Tools))
	}
	if ctx.Tools[0].ToolSpecification.Name != "bash" {
		t.Fatalf("expected tool name 'bash', got %q", ctx.Tools[0].ToolSpecification.Name)
	}
}

func TestCollectHistoryToolsSanitizesNames(t *testing.T) {
	// Verify that auto-declared tool names go through sanitizeToolName.
	req := &ClaudeRequest{
		Model: "claude-sonnet-4.5",
		Messages: []ClaudeMessage{
			{Role: "user", Content: "Do something"},
			{Role: "assistant", Content: []interface{}{
				map[string]interface{}{"type": "tool_use", "id": "call_1", "name": "mcp__server__long_tool_name", "input": map[string]interface{}{}},
			}},
			{Role: "user", Content: []interface{}{
				map[string]interface{}{"type": "tool_result", "tool_use_id": "call_1", "content": "ok"},
			}},
			{Role: "assistant", Content: "Done."},
			{Role: "user", Content: "Summarize."},
		},
		Tools: nil,
	}

	payload := ClaudeToKiro(req, false)

	ctx := payload.ConversationState.CurrentMessage.UserInputMessage.UserInputMessageContext
	if ctx == nil || len(ctx.Tools) == 0 {
		t.Fatalf("expected auto-declared tools")
	}

	// "mcp__server__long_tool_name" should be sanitized (underscores -> camelCase)
	toolName := ctx.Tools[0].ToolSpecification.Name
	if strings.Contains(toolName, "_") {
		t.Fatalf("expected sanitized tool name without underscores, got %q", toolName)
	}

	// ToolNameMap should map sanitized -> original
	if payload.ToolNameMap == nil {
		t.Fatalf("expected ToolNameMap to be set for sanitized names")
	}
	if payload.ToolNameMap[toolName] != "mcp__server__long_tool_name" {
		t.Fatalf("expected ToolNameMap[%q] = 'mcp__server__long_tool_name', got %q", toolName, payload.ToolNameMap[toolName])
	}
}

func TestCollectHistoryToolsNoopWhenHistoryHasNoTools(t *testing.T) {
	// Pure text conversation with no tools in history - should not inject any tools.
	req := &ClaudeRequest{
		Model: "claude-sonnet-4.5",
		Messages: []ClaudeMessage{
			{Role: "user", Content: "Hello"},
			{Role: "assistant", Content: "Hi there"},
			{Role: "user", Content: "How are you?"},
		},
		Tools: nil,
	}

	payload := ClaudeToKiro(req, false)

	ctx := payload.ConversationState.CurrentMessage.UserInputMessage.UserInputMessageContext
	if ctx != nil && len(ctx.Tools) > 0 {
		t.Fatalf("expected no tools for pure text conversation, got %d tools", len(ctx.Tools))
	}
}

func TestParseModelAndThinkingDoesNotRewriteDatedSnapshotMinor(t *testing.T) {
	// Guards the \b boundary in claudeVersionPattern: without it, the regex would
	// rewrite "claude-sonnet-4-20250514" to "claude-sonnet-4.20250514" before the
	// alias table could redirect it.
	got, _ := ParseModelAndThinking("claude-sonnet-4-20250514", "-thinking")
	if got != "claude-sonnet-4" {
		t.Fatalf("dated snapshot must alias to claude-sonnet-4, got %q", got)
	}
	if strings.Contains(got, ".") {
		t.Fatalf("dated snapshot must not be rewritten with a dot, got %q", got)
	}
}
