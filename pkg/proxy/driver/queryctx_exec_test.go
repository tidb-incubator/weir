package driver

import (
	"testing"

	"github.com/pingcap/parser"
	"github.com/stretchr/testify/assert"
)

func TestFirstTableNameVisitor_TableName(t *testing.T) {
	type fields struct {
		sql  string
		want string
	}
	tests := []fields{
		{sql: "SELECT 1", want: ""},
		{sql: "SELECT * FROM tbl1", want: "tbl1"},
		{sql: "SELECT * FROM tbl1,tbl2", want: "tbl1"},
		{sql: "SELECT * FROM tbl1 INNER JOIN tbl2 on tbl1.a = tbl2.a", want: "tbl1"},
		{sql: "INSERT INTO tbl1 VALUES (1,2,3)", want: "tbl1"},
		{sql: "DELETE FROM tbl1 WHERE id=1", want: "tbl1"},
		{sql: "UPDATE tbl1 SET a=1 WHERE id=1", want: "tbl1"},
	}
	for _, tt := range tests {
		t.Run(tt.sql, func(t *testing.T) {
			stmt, err := parser.New().ParseOneStmt(tt.sql, "", "")
			f := &FirstTableNameVisitor{}
			stmt.Accept(f)
			assert.NoError(t, err)
			assert.Equal(t, tt.want, f.TableName())
		})
	}
}
