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
