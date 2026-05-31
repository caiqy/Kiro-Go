package proxy

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"kiro-go/config"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func TestNormalizeChunkBasicProgression(t *testing.T) {
	prev := ""

	if got := normalizeChunk("abc", &prev); got != "abc" {
		t.Fatalf("expected first chunk to pass through, got %q", got)
	}
	if got := normalizeChunk("abcde", &prev); got != "de" {
		t.Fatalf("expected appended delta, got %q", got)
	}
}

func TestNormalizeChunkPrefixRewindDoesNotReplay(t *testing.T) {
	prev := ""

	_ = normalizeChunk("abcde", &prev)
	if got := normalizeChunk("abc", &prev); got != "" {
		t.Fatalf("expected rewind chunk to be ignored, got %q", got)
	}
	if prev != "abcde" {
		t.Fatalf("expected previous snapshot to remain longest version, got %q", prev)
	}
	if got := normalizeChunk("abcdef", &prev); got != "f" {
		t.Fatalf("expected only unseen suffix after rewind, got %q", got)
	}
}

func TestNormalizeChunkNonCumulativeReturnsFullChunk(t *testing.T) {
	prev := "hello world"

	// 非累积 chunk 应完整返回，不做重叠猜测（避免重复模式误判吞内容）
	if got := normalizeChunk("world!!!", &prev); got != "world!!!" {
		t.Fatalf("expected full chunk for non-cumulative input, got %q", got)
	}
}

func TestNormalizeChunkMarkdownTableSeparator(t *testing.T) {
	// 回归测试：markdown 表格分隔行的重复模式不应被吞掉
	prev := "| # | 域名 | 下载 | 上传 | 总计 | 连接数 |\n|---|------"

	chunk := "|------|------|------|------|\n| 1"
	got := normalizeChunk(chunk, &prev)
	if got != chunk {
		t.Fatalf("expected full chunk (table separator must not be eaten), got %q", got)
	}
}

func TestNormalizeChunkExactDuplicate(t *testing.T) {
	prev := ""

	_ = normalizeChunk("hello world", &prev)
	// 完全重复的 chunk 应返回空
	if got := normalizeChunk("hello world", &prev); got != "" {
		t.Fatalf("expected empty for exact duplicate, got %q", got)
	}
}

func TestNormalizeChunkFallbackUpdatesState(t *testing.T) {
	prev := ""

	_ = normalizeChunk("abc", &prev)
	// 非累积 chunk（与 prev 无前缀关系）
	got := normalizeChunk("xyz", &prev)
	if got != "xyz" {
		t.Fatalf("expected full chunk on fallback, got %q", got)
	}
	// fallback 后 previous 应更新为新 chunk，后续累积增长基于新内容
	if prev != "xyz" {
		t.Fatalf("expected previous updated to new chunk, got %q", prev)
	}
	if got := normalizeChunk("xyz123", &prev); got != "123" {
		t.Fatalf("expected normal delta after fallback, got %q", got)
	}
}

func TestParseEventStreamAssistantAndReasoningIndependent(t *testing.T) {
	// assistant 和 reasoning 各自维护独立的 normalize 状态，不互相干扰
	stream := bytes.NewReader(bytes.Join([][]byte{
		awsEventStreamFrame(t, "assistantResponseEvent", map[string]interface{}{"content": "hello"}),
		awsEventStreamFrame(t, "reasoningContentEvent", map[string]interface{}{"text": "think"}),
		awsEventStreamFrame(t, "assistantResponseEvent", map[string]interface{}{"content": "hello world"}),
		awsEventStreamFrame(t, "reasoningContentEvent", map[string]interface{}{"text": "thinking"}),
	}, nil))

	var assistantTexts []string
	var reasoningTexts []string
	err := parseEventStream(stream, &KiroStreamCallback{
		OnText: func(text string, isThinking bool) {
			if isThinking {
				reasoningTexts = append(reasoningTexts, text)
			} else {
				assistantTexts = append(assistantTexts, text)
			}
		},
		OnComplete: func(_, _ int) {},
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// assistant: "hello" -> "hello world" 应产生 "hello" + " world"
	expectedAssistant := []string{"hello", " world"}
	if len(assistantTexts) != len(expectedAssistant) {
		t.Fatalf("assistant texts: expected %v, got %v", expectedAssistant, assistantTexts)
	}
	for i, exp := range expectedAssistant {
		if assistantTexts[i] != exp {
			t.Fatalf("assistant[%d]: expected %q, got %q", i, exp, assistantTexts[i])
		}
	}

	// reasoning: "think" -> "thinking" 应产生 "think" + "ing"
	expectedReasoning := []string{"think", "ing"}
	if len(reasoningTexts) != len(expectedReasoning) {
		t.Fatalf("reasoning texts: expected %v, got %v", expectedReasoning, reasoningTexts)
	}
	for i, exp := range expectedReasoning {
		if reasoningTexts[i] != exp {
			t.Fatalf("reasoning[%d]: expected %q, got %q", i, exp, reasoningTexts[i])
		}
	}
}

func TestParseEventStreamFinishesPendingToolUseOnEOF(t *testing.T) {
	stream := bytes.NewReader(awsEventStreamFrame(t, "toolUseEvent", map[string]interface{}{
		"toolUseId": "toolu_1",
		"name":      "mcpIdaProMcpStatus",
		"input":     `{"server":"ida-pro-mcp"}`,
	}))

	var toolUses []KiroToolUse
	var completed bool
	err := parseEventStream(stream, &KiroStreamCallback{
		OnToolUse: func(toolUse KiroToolUse) {
			toolUses = append(toolUses, toolUse)
		},
		OnComplete: func(_, _ int) {
			completed = true
		},
	})
	if err != nil {
		t.Fatalf("unexpected parse error: %v", err)
	}
	if !completed {
		t.Fatalf("expected stream completion callback")
	}
	if len(toolUses) != 1 {
		t.Fatalf("expected pending tool use to be emitted on EOF, got %d", len(toolUses))
	}
	if toolUses[0].ToolUseID != "toolu_1" || toolUses[0].Name != "mcpIdaProMcpStatus" {
		t.Fatalf("unexpected tool use: %#v", toolUses[0])
	}
	if got := toolUses[0].Input["server"]; got != "ida-pro-mcp" {
		t.Fatalf("expected parsed tool input, got %#v", toolUses[0].Input)
	}
}

func TestParseEventStreamNilCallbackIsNoOp(t *testing.T) {
	stream := bytes.NewReader(bytes.Join([][]byte{
		awsEventStreamFrame(t, "assistantResponseEvent", map[string]interface{}{"content": "hello"}),
		awsEventStreamFrame(t, "reasoningContentEvent", map[string]interface{}{"text": "thinking"}),
		awsEventStreamFrame(t, "contextUsageEvent", map[string]interface{}{"contextUsagePercentage": 12.5}),
		awsEventStreamFrame(t, "meteringEvent", map[string]interface{}{"usage": 1.25}),
		awsEventStreamFrame(t, "toolUseEvent", map[string]interface{}{
			"name":  "mcpIdaProMcpStatus",
			"input": `{"server":"ida-pro-mcp"}`,
			"stop":  true,
		}),
	}, nil))

	if err := parseEventStream(stream, nil); err != nil {
		t.Fatalf("expected nil callback to be a no-op, got %v", err)
	}
}

func TestParseEventStreamNilCallbackFieldsAreNoOp(t *testing.T) {
	stream := bytes.NewReader(awsEventStreamFrame(t, "assistantResponseEvent", map[string]interface{}{
		"content": "hello",
	}))

	if err := parseEventStream(stream, &KiroStreamCallback{}); err != nil {
		t.Fatalf("expected empty callback to be a no-op, got %v", err)
	}
}

func TestHandleToolUseEventGeneratesMissingToolUseID(t *testing.T) {
	var toolUses []KiroToolUse
	current := handleToolUseEvent(map[string]interface{}{
		"name":  "mcpIdaProMcpStatus",
		"input": `{"server":"ida-pro-mcp"}`,
		"stop":  true,
	}, nil, &KiroStreamCallback{
		OnToolUse: func(toolUse KiroToolUse) {
			toolUses = append(toolUses, toolUse)
		},
	})

	if current != nil {
		t.Fatalf("expected stopped tool use to clear current state")
	}
	if len(toolUses) != 1 {
		t.Fatalf("expected one tool use, got %d", len(toolUses))
	}
	if toolUses[0].ToolUseID == "" {
		t.Fatalf("expected generated tool use id")
	}
	if toolUses[0].Name != "mcpIdaProMcpStatus" {
		t.Fatalf("unexpected tool name: %q", toolUses[0].Name)
	}
}

func TestHandleToolUseEventReplacesGeneratedIDWhenRealIDArrives(t *testing.T) {
	var toolUses []KiroToolUse
	callback := &KiroStreamCallback{
		OnToolUse: func(toolUse KiroToolUse) {
			toolUses = append(toolUses, toolUse)
		},
	}

	current := handleToolUseEvent(map[string]interface{}{
		"name":  "mcpIdaProMcpStatus",
		"input": `{"server":`,
	}, nil, callback)
	current = handleToolUseEvent(map[string]interface{}{
		"toolUseId": "toolu_real",
		"name":      "mcpIdaProMcpStatus",
		"input":     `"ida-pro-mcp"}`,
		"stop":      true,
	}, current, callback)

	if current != nil {
		t.Fatalf("expected stopped tool use to clear current state")
	}
	if len(toolUses) != 1 {
		t.Fatalf("expected one completed tool use, got %d", len(toolUses))
	}
	if toolUses[0].ToolUseID != "toolu_real" {
		t.Fatalf("expected real tool id to replace generated id, got %q", toolUses[0].ToolUseID)
	}
	if got := toolUses[0].Input["server"]; got != "ida-pro-mcp" {
		t.Fatalf("expected joined tool input, got %#v", toolUses[0].Input)
	}
}

func TestBuildKiroTransportUsesExplicitProxyURL(t *testing.T) {
	transport := buildKiroTransport("http://proxy.local:8080")
	req := &http.Request{URL: mustParseURL(t, "https://q.us-east-1.amazonaws.com")}

	got, err := transport.Proxy(req)
	if err != nil {
		t.Fatalf("unexpected proxy error: %v", err)
	}
	assertProxyURL(t, got, "http://proxy.local:8080")
}

func TestBuildKiroTransportFallsBackToEnvironmentProxy(t *testing.T) {
	t.Setenv("HTTPS_PROXY", "http://env-proxy.local:2323")
	t.Setenv("NO_PROXY", "")
	t.Setenv("no_proxy", "")

	transport := buildKiroTransport("")
	req := &http.Request{URL: mustParseURL(t, "https://q.us-east-1.amazonaws.com")}

	got, err := transport.Proxy(req)
	if err != nil {
		t.Fatalf("unexpected proxy error: %v", err)
	}
	assertProxyURL(t, got, "http://env-proxy.local:2323")
}

func TestInitKiroHttpClientKeepsShortRestTimeout(t *testing.T) {
	InitKiroHttpClient("")
	t.Cleanup(func() { InitKiroHttpClient("") })

	streamClient := kiroHttpStore.Load()
	restClient := kiroRestHttpStore.Load()

	if streamClient.Timeout != 5*time.Minute {
		t.Fatalf("expected streaming timeout to be 5m, got %s", streamClient.Timeout)
	}
	if restClient.Timeout != 30*time.Second {
		t.Fatalf("expected REST timeout to stay 30s, got %s", restClient.Timeout)
	}
}

func TestSetPayloadProfileArnForAccountUsesAccountArn(t *testing.T) {
	payload := &KiroPayload{ProfileArn: "arn:aws:codewhisperer:profile/stale"}

	setPayloadProfileArnForAccount(payload, &config.Account{ProfileArn: " arn:aws:codewhisperer:profile/current "})
	if payload.ProfileArn != "arn:aws:codewhisperer:profile/current" {
		t.Fatalf("expected current account profile ARN, got %q", payload.ProfileArn)
	}
}

func TestSetPayloadProfileArnForAccountPreservesExplicitPayloadArn(t *testing.T) {
	payload := &KiroPayload{ProfileArn: " arn:aws:codewhisperer:profile/explicit "}

	setPayloadProfileArnForAccount(payload, &config.Account{})
	if payload.ProfileArn != "arn:aws:codewhisperer:profile/explicit" {
		t.Fatalf("expected explicit payload profile ARN to be preserved, got %q", payload.ProfileArn)
	}
}

func mustParseURL(t *testing.T, raw string) *url.URL {
	t.Helper()
	parsed, err := url.Parse(raw)
	if err != nil {
		t.Fatalf("invalid test URL: %v", err)
	}
	return parsed
}

func assertProxyURL(t *testing.T, got *url.URL, want string) {
	t.Helper()
	if got == nil {
		t.Fatalf("expected proxy URL %q, got nil", want)
	}
	if got.String() != want {
		t.Fatalf("expected proxy URL %q, got %q", want, got.String())
	}
}

func awsEventStreamFrame(t *testing.T, eventType string, payload map[string]interface{}) []byte {
	t.Helper()

	payloadBytes, err := json.Marshal(payload)
	if err != nil {
		t.Fatalf("marshal payload: %v", err)
	}

	headerValue := []byte(eventType)
	headers := make([]byte, 0, 1+len(":event-type")+1+2+len(headerValue))
	headers = append(headers, byte(len(":event-type")))
	headers = append(headers, []byte(":event-type")...)
	headers = append(headers, byte(7))
	headers = append(headers, byte(len(headerValue)>>8), byte(len(headerValue)))
	headers = append(headers, headerValue...)

	totalLength := 12 + len(headers) + len(payloadBytes) + 4
	frame := make([]byte, 12, totalLength)
	binary.BigEndian.PutUint32(frame[0:4], uint32(totalLength))
	binary.BigEndian.PutUint32(frame[4:8], uint32(len(headers)))
	frame = append(frame, headers...)
	frame = append(frame, payloadBytes...)
	frame = append(frame, 0, 0, 0, 0)
	return frame
}

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

// 多工具（每个事件带显式 toolUseId）：两个工具都应在各自第一个片段时触发 Start，
// 即第二个工具的 Start 必须发生在它的 stop 之前（不退化为非流式）。
type toolEvt struct {
	kind string // "start" | "delta" | "use"
	id   string
}

func TestHandleToolUseEventMultipleToolsWithExplicitIDsStreamEach(t *testing.T) {
	var seq []toolEvt
	cb := &KiroStreamCallback{
		OnToolUseStart: func(id, name string) { seq = append(seq, toolEvt{"start", id}) },
		OnToolUseDelta: func(id, pj string) { seq = append(seq, toolEvt{"delta", id}) },
		OnToolUse:      func(tu KiroToolUse) { seq = append(seq, toolEvt{"use", tu.ToolUseID}) },
	}

	var cur *toolUseState
	// tool A
	cur = handleToolUseEvent(map[string]interface{}{"toolUseId": "A", "name": "fa", "input": `{"x":`}, cur, cb)
	cur = handleToolUseEvent(map[string]interface{}{"toolUseId": "A", "input": `1}`, "stop": true}, cur, cb)
	// tool B
	cur = handleToolUseEvent(map[string]interface{}{"toolUseId": "B", "name": "fb", "input": `{"y":`}, cur, cb)
	cur = handleToolUseEvent(map[string]interface{}{"toolUseId": "B", "input": `2}`, "stop": true}, cur, cb)

	// 期望顺序：A start, A delta, A delta, A use, B start, B delta, B delta, B use
	want := []toolEvt{
		{"start", "A"}, {"delta", "A"}, {"delta", "A"}, {"use", "A"},
		{"start", "B"}, {"delta", "B"}, {"delta", "B"}, {"use", "B"},
	}
	if len(seq) != len(want) {
		t.Fatalf("event count mismatch: got %d want %d (%#v)", len(seq), len(want), seq)
	}
	for i := range want {
		if seq[i] != want[i] {
			t.Fatalf("at %d: got %+v want %+v (full=%#v)", i, seq[i], want[i], seq)
		}
	}
}

// 多工具（仅靠 name 变化切换，无显式 toolUseId）：第二个工具走 GeneratedID 分支。
// 验证第二个工具仍能在 stop 前增量发送（Start 在第一个片段时，而非退化到 stop 才一次性发）。
func TestHandleToolUseEventMultipleToolsByNameSwitchStreamEach(t *testing.T) {
	var seq []string // "start:<name>" | "delta" | "use:<name>"
	cb := &KiroStreamCallback{
		OnToolUseStart: func(id, name string) { seq = append(seq, "start:"+name) },
		OnToolUseDelta: func(id, pj string) { seq = append(seq, "delta") },
		OnToolUse:      func(tu KiroToolUse) { seq = append(seq, "use:"+tu.Name) },
	}

	var cur *toolUseState
	// tool fa (name only)
	cur = handleToolUseEvent(map[string]interface{}{"name": "fa", "input": `{"x":`}, cur, cb)
	cur = handleToolUseEvent(map[string]interface{}{"input": `1}`}, cur, cb)
	// switch to fb by name change (no stop on fa)
	cur = handleToolUseEvent(map[string]interface{}{"name": "fb", "input": `{"y":`}, cur, cb)
	cur = handleToolUseEvent(map[string]interface{}{"input": `2}`, "stop": true}, cur, cb)

	// 第二个工具 fb 的 start 必须出现在它的 use 之前（即不退化为一次性）。
	startFbIdx, useFbIdx := -1, -1
	for i, e := range seq {
		if e == "start:fb" && startFbIdx < 0 {
			startFbIdx = i
		}
		if e == "use:fb" && useFbIdx < 0 {
			useFbIdx = i
		}
	}
	if startFbIdx < 0 {
		t.Fatalf("fb never emitted start: %#v", seq)
	}
	if useFbIdx < 0 {
		t.Fatalf("fb never emitted use: %#v", seq)
	}
	// 关键断言：fb 的 start 与 use 之间应至少有一个 delta（增量），且 start 在 use 前。
	if startFbIdx >= useFbIdx {
		t.Fatalf("fb start should precede use, got start@%d use@%d: %#v", startFbIdx, useFbIdx, seq)
	}
}

// 协议不变量：对每个工具，OnToolUseStart 必须先于该工具的任何 OnToolUseDelta。
// 否则 handler 收到 delta 时块尚未开始（toolBlockIndex<0 / curFcID==""），delta 被丢弃，
// 导致工具参数丢失。本测试覆盖 GeneratedID（仅 name、无显式 toolUseId）路径。
func TestHandleToolUseEventStartPrecedesDeltaForGeneratedID(t *testing.T) {
	var seq []string // "start" | "delta"
	cb := &KiroStreamCallback{
		OnToolUseStart: func(id, name string) { seq = append(seq, "start") },
		OnToolUseDelta: func(id, pj string) { seq = append(seq, "delta") },
		OnToolUse:      func(tu KiroToolUse) {},
	}

	var cur *toolUseState
	cur = handleToolUseEvent(map[string]interface{}{"name": "fa", "input": `{"x":`}, cur, cb)
	cur = handleToolUseEvent(map[string]interface{}{"input": `1}`, "stop": true}, cur, cb)

	if len(seq) == 0 {
		t.Fatalf("expected events")
	}
	// 第一个事件必须是 start，不能是 delta。
	if seq[0] != "start" {
		t.Fatalf("OnToolUseStart must precede any OnToolUseDelta, got order: %#v", seq)
	}
}



