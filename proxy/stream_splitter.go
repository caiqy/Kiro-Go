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

// streamTagSplitter 将上游 OnText 流切分为正文/思考片段并通过 emit 即时下发。
// 它在保证不泄漏被切断的半截 <thinking>/</thinking> 标签的前提下，尽量立即下发文本，
// 避免固定大阈值缓冲导致的流式卡顿。
//
// emit 的 state 含义：0=正文文本，1=思考开始，2=思考续写，3=思考结束。
type streamTagSplitter struct {
	thinkingEnabled bool
	emit            func(text string, state int)

	buf               string
	inThinking        bool
	dropThinking      bool
	thinkingStarted   bool
	eventThinkingOpen bool
	source            thinkingStreamSource
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
