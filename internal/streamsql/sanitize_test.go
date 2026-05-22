package streamsql

import (
	"strings"
	"testing"
)

func TestRejectUnsafeSQL(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		sql       string
		wantErr   bool
		errSubstr string
	}{
		{"simple select", "SELECT * FROM topic", false, ""},
		{"select with WHERE", "SELECT id FROM orders WHERE amount > 100", false, ""},
		{"select with GROUP BY", "SELECT region, COUNT(*) FROM orders GROUP BY region", false, ""},
		{"reject semicolon multi-statement", "SELECT * FROM t1; SELECT * FROM t2", true, "multiple SQL statements"},
		{"allow trailing semicolon", "SELECT * FROM t1;", false, ""},
		{"reject inline comment", "SELECT * FROM t1 -- evil", true, "inline comments"},
		{"reject block comment", "SELECT /* evil */ * FROM t1", true, "block comments"},
		{"reject INSERT keyword", "INSERT INTO t1 VALUES (1)", true, "INSERT"},
		{"reject UPDATE keyword", "UPDATE t1 SET x=1", true, "UPDATE"},
		{"reject DELETE keyword", "DELETE FROM t1", true, "DELETE"},
		{"reject DROP keyword", "DROP TABLE t1", true, "DROP"},
		{"reject GRANT keyword", "GRANT ALL ON t1 TO 'x'", true, "GRANT"},
		{"reject EXEC keyword", "EXEC sp_evil", true, "EXEC"},
		{"reject embedded DROP in WHERE", "SELECT * FROM t WHERE x = 'a' OR DROP TABLE t", true, "DROP"},
		{"allow column named dropped", "SELECT dropped FROM t1", false, ""},
		{"allow column named inserted_at", "SELECT inserted_at FROM events", false, ""},
		{"case insensitive: drop", "drop table t1", true, "DROP"},
	}

	for _, tc := range tests {
		tc := tc
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			err := rejectUnsafeSQL(tc.sql)
			if (err != nil) != tc.wantErr {
				t.Fatalf("rejectUnsafeSQL(%q) error = %v, wantErr %v", tc.sql, err, tc.wantErr)
			}
			if tc.errSubstr != "" && err != nil && !strings.Contains(err.Error(), tc.errSubstr) {
				t.Errorf("error %q does not contain expected substring %q", err.Error(), tc.errSubstr)
			}
		})
	}
}

func TestEngineParse_RejectsUnsafeSQL(t *testing.T) {
	t.Parallel()
	e := NewEngine(nil)

	if _, err := e.Parse("DROP TABLE topic_a", "agent"); err == nil {
		t.Error("expected DROP to be rejected, got nil error")
	}
	if _, err := e.Parse("SELECT * FROM t; SELECT * FROM evil", "agent"); err == nil {
		t.Error("expected multi-statement SQL to be rejected, got nil error")
	}
	if _, err := e.Parse("SELECT /* hack */ * FROM t", "agent"); err == nil {
		t.Error("expected block comment to be rejected, got nil error")
	}
	if _, err := e.Parse(strings.Repeat("A", maxSQLLen+1), "agent"); err == nil {
		t.Error("expected oversized SQL to be rejected, got nil error")
	}
}
