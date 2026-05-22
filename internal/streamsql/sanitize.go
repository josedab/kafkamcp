package streamsql

import (
	"fmt"
	"regexp"
	"strings"
)

const (
	// maxSQLLen caps the length of an incoming SQL string. Real
	// streaming SELECTs are well under 1 KiB; capping protects the
	// regex parser from catastrophic backtracking on attacker-
	// supplied input and reduces the amount of memory the engine
	// will hold per query.
	maxSQLLen = 4096
)

// disallowedKeywords contains SQL verbs that have no business
// appearing in a streaming SELECT. The streaming engine only consumes
// Kafka topics — there is no underlying database for DDL/DML to
// affect — but rejecting these keywords up-front gives a clear error
// to callers who mis-route a normal SQL request to KafkaMCP, and it
// hardens defense-in-depth in case the parser is ever swapped for a
// real one in the future.
var disallowedKeywords = []string{
	"INSERT", "UPDATE", "DELETE", "MERGE",
	"DROP", "TRUNCATE", "ALTER", "CREATE",
	"GRANT", "REVOKE", "EXEC", "EXECUTE", "CALL",
	"ATTACH", "DETACH",
}

// inlineCommentRe and blockCommentRe match SQL comment forms. The
// streaming parser ignores them today, but allowing comments through
// makes future bypasses easier (e.g. `SELECT/*;DROP*/ * FROM t`) and
// they are never necessary in a tool-call payload.
var (
	inlineCommentRe = regexp.MustCompile(`--`)
	blockCommentRe  = regexp.MustCompile(`/\*|\*/`)
	multiStmtRe     = regexp.MustCompile(`;\s*\S`)
)

// rejectUnsafeSQL rejects obviously dangerous SQL fragments before
// the regex-based parser tries to interpret them. It is intentionally
// strict: false positives are cheap (caller adjusts their query),
// false negatives could let a malicious agent escape the read-only
// streaming contract. Called once at Parse time.
func rejectUnsafeSQL(sql string) error {
	if multiStmtRe.MatchString(sql) {
		return fmt.Errorf("multiple SQL statements are not allowed (found ';')")
	}
	if inlineCommentRe.MatchString(sql) {
		return fmt.Errorf("inline comments ('--') are not allowed")
	}
	if blockCommentRe.MatchString(sql) {
		return fmt.Errorf("block comments ('/* */') are not allowed")
	}
	upper := strings.ToUpper(sql)
	for _, kw := range disallowedKeywords {
		// Look for the keyword as a standalone token (surrounded by
		// whitespace or string boundaries) to avoid flagging columns
		// like `dropped` or `inserted_at`.
		if containsWord(upper, kw) {
			return fmt.Errorf("disallowed SQL keyword %q: this endpoint only accepts read-only SELECT queries", kw)
		}
	}
	return nil
}

// containsWord reports whether upper contains word as a standalone
// keyword (bounded by non-alphanumerics or string ends). Both upper
// and word are expected to be upper-case ASCII.
func containsWord(upper, word string) bool {
	for {
		idx := strings.Index(upper, word)
		if idx < 0 {
			return false
		}
		left := idx == 0 || !isWordByte(upper[idx-1])
		right := idx+len(word) == len(upper) || !isWordByte(upper[idx+len(word)])
		if left && right {
			return true
		}
		upper = upper[idx+1:]
	}
}

func isWordByte(b byte) bool {
	return (b >= 'A' && b <= 'Z') || (b >= '0' && b <= '9') || b == '_'
}
