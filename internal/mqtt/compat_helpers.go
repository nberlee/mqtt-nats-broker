// Helpers vendored (verbatim or lightly adapted) from github.com/nats-io/nats-server/v2
// (Apache-2.0). See NOTICE for attribution. These are self-contained utilities the MQTT
// core relies on; they carry no NATS-server runtime coupling.

package mqtt

import (
	"bytes"
	"crypto/sha256"
	"time"
	"unsafe"
)

// String constants (nats-server: route.go, client.go, jetstream_api.go, sublist.go).
const (
	_EMPTY_ = ""
	_CRLF_  = "\r\n"

	hdrLine  = "NATS/1.0\r\n"
	jsAckPre = "$JS.ACK."

	pwc   = '*'
	fwc   = '>'
	btsep = '.'
)

// Hashing (nats-server: events.go, accounts.go). getHash yields a stable, collision-resistant
// short token used for session/consumer/stream naming.
const (
	digits     = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
	base       = 62
	sysHashLen = 8
)

func getHash(name string) string { return getHashSize(name, sysHashLen) }

func getHashSize(name string, size int) string {
	sha := sha256.New()
	sha.Write([]byte(name))
	b := sha.Sum(nil)
	for i := 0; i < size; i++ {
		b[i] = digits[int(b[i]%base)]
	}
	return string(b[:size])
}

// Zero-copy string/byte conversions (nats-server: store.go).
func bytesToString(b []byte) string {
	if len(b) == 0 {
		return _EMPTY_
	}
	p := unsafe.SliceData(b)
	return unsafe.String(p, len(b))
}

func stringToBytes(s string) []byte {
	if len(s) == 0 {
		return nil
	}
	p := unsafe.StringData(s)
	return unsafe.Slice(p, len(s))
}

func copyBytes(src []byte) []byte {
	if len(src) == 0 {
		return nil
	}
	dst := make([]byte, len(src))
	copy(dst, src)
	return dst
}

func secondsToDuration(seconds float64) time.Duration {
	return time.Duration(seconds * float64(time.Second))
}

// Subject token access (nats-server: sublist.go).
func tokenAt(subject string, index uint8) string {
	ti, start := uint8(1), 0
	for i := 0; i < len(subject); i++ {
		if subject[i] == btsep {
			if ti == index {
				return subject[start:i]
			}
			start = i + 1
			ti++
		}
	}
	if ti == index {
		return subject[start:]
	}
	return _EMPTY_
}

// JetStream ack-reply subject parsing (nats-server: consumer.go).
const (
	expectedNumReplyTokensV1 = 9
	expectedNumReplyTokensV2 = 11
)

func ackReplyInfo(subject string) (sseq, dseq, dc uint64, ts int64, pending uint64) {
	start, tokens := 0, make([]string, 0, expectedNumReplyTokensV2)
	for i := 0; i < len(subject); i++ {
		if subject[i] == btsep {
			tokens = append(tokens, subject[start:i])
			start = i + 1
		}
	}
	tokens = append(tokens, subject[start:])
	if (len(tokens) != expectedNumReplyTokensV1 && len(tokens) < expectedNumReplyTokensV2) || tokens[0] != "$JS" || tokens[1] != "ACK" {
		return 0, 0, 0, 0, 0
	}
	offset := 2
	if len(tokens) >= expectedNumReplyTokensV2 {
		offset = 4
	}
	dc = uint64(parseAckReplyNum(tokens[offset+2]))
	sseq, dseq = uint64(parseAckReplyNum(tokens[offset+3])), uint64(parseAckReplyNum(tokens[offset+4]))
	ts = parseAckReplyNum(tokens[offset+5])
	pending = uint64(parseAckReplyNum(tokens[offset+6]))
	return sseq, dseq, dc, ts, pending
}

func parseAckReplyNum(d string) (n int64) {
	if len(d) == 0 {
		return -1
	}
	for _, dec := range d {
		if dec < '0' || dec > '9' {
			return -1
		}
		n = n*10 + (int64(dec) - '0')
	}
	return n
}

// NATS message-header access (nats-server: client.go).
func getHeader(key string, hdr []byte) []byte {
	v := sliceHeader(key, hdr)
	if v == nil {
		return nil
	}
	return append(make([]byte, 0, len(v)), v...)
}

func sliceHeader(key string, hdr []byte) []byte {
	if len(hdr) == 0 {
		return nil
	}
	index := getHeaderKeyIndex(key, hdr)
	if index == -1 {
		return nil
	}
	index += len(key) + 1
	hdrLen := len(hdr)
	for index < hdrLen && hdr[index] == ' ' {
		index++
	}
	start := index
	for index < hdrLen {
		if hdr[index] == '\r' && index < hdrLen-1 && hdr[index+1] == '\n' {
			break
		}
		index++
	}
	return hdr[start:index:index]
}

func getHeaderKeyIndex(key string, hdr []byte) int {
	if len(hdr) == 0 {
		return -1
	}
	bkey := stringToBytes(key)
	keyLen, hdrLen := len(key), len(hdr)
	var offset int
	for {
		index := bytes.Index(hdr[offset:], bkey)
		if index < 2 {
			return -1
		}
		index += offset
		if index >= hdrLen-1 || hdr[index-1] != '\n' || hdr[index-2] != '\r' {
			offset = index + keyLen
			continue
		}
		if index+keyLen >= hdrLen {
			return -1
		}
		if hdr[index+keyLen] != ':' {
			offset = index + keyLen
			continue
		}
		return index
	}
}
