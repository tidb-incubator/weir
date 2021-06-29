package driver

import (
	"context"
	"fmt"
	"sync/atomic"

	"github.com/tidb-incubator/weir/pkg/proxy/server"
	"github.com/pingcap/tidb/types"
	"github.com/pingcap/tidb/util/chunk"
	"github.com/pingcap/tidb/util/hack"
	"github.com/siddontang/go-mysql/mysql"
)

type weirResultSet struct {
	result      *mysql.Result
	columnInfos []*server.ColumnInfo
	closed      int32
	readed      bool
}

func wrapMySQLResult(result *mysql.Result) *weirResultSet {
	resultSet := &weirResultSet{
		result: result,
	}
	columnInfos := convertFieldsToColumnInfos(result.Fields)
	resultSet.columnInfos = columnInfos
	return resultSet
}

func createBinaryPrepareColumns(cnt int) []*server.ColumnInfo {
	info := &server.ColumnInfo{}
	return copyColumnInfo(info, cnt)
}

func createBinaryPrepareParams(cnt int) []*server.ColumnInfo {
	info := &server.ColumnInfo{Name: "?"}
	return copyColumnInfo(info, cnt)
}

func copyColumnInfo(info *server.ColumnInfo, cnt int) []*server.ColumnInfo {
	ret := make([]*server.ColumnInfo, 0, cnt)
	for i := 0; i < cnt; i++ {
		ret = append(ret, info)
	}
	return ret
}

func convertFieldsToColumnInfos(fields []*mysql.Field) []*server.ColumnInfo {
	var columnInfos []*server.ColumnInfo
	for _, field := range fields {
		columnInfo := &server.ColumnInfo{
			Schema:             string(hack.String(field.Schema)),
			Table:              string(hack.String(field.Table)),
			OrgTable:           string(hack.String(field.OrgTable)),
			Name:               string(hack.String(field.Name)),
			OrgName:            string(hack.String(field.OrgName)),
			ColumnLength:       field.ColumnLength,
			Charset:            field.Charset,
			Flag:               field.Flag,
			Decimal:            field.Decimal,
			Type:               field.Type,
			DefaultValueLength: field.DefaultValueLength,
			DefaultValue:       field.DefaultValue,
		}
		columnInfos = append(columnInfos, columnInfo)
	}
	return columnInfos
}

func convertFieldTypes(fields []*mysql.Field) []*types.FieldType {
	var ret []*types.FieldType
	for _, f := range fields {
		ft := types.NewFieldType(f.Type)
		ft.Flag = uint(f.Flag)
		ret = append(ret, ft)
	}
	return ret
}

func writeResultSetDataToTrunk(r *mysql.Resultset, c *chunk.Chunk) {
	for _, rowValue := range r.Values {
		for colIdx, colValue := range rowValue {
			switch colValue.Type {
			case mysql.FieldValueTypeNull:
				c.Column(colIdx).AppendNull()
			case mysql.FieldValueTypeUnsigned:
				c.Column(colIdx).AppendUint64(colValue.AsUint64())
			case mysql.FieldValueTypeSigned:
				c.Column(colIdx).AppendInt64(colValue.AsInt64())
			case mysql.FieldValueTypeFloat:
				c.Column(colIdx).AppendFloat64(colValue.AsFloat64())
			case mysql.FieldValueTypeString:
				c.Column(colIdx).AppendBytes(colValue.AsString())
			default:
				panic(fmt.Errorf("invalid col value type: %v", colValue.Type))
			}
		}
	}
}

func (w *weirResultSet) Columns() []*server.ColumnInfo {
	return w.columnInfos
}

func (w *weirResultSet) NewChunk() *chunk.Chunk {
	columns := convertFieldTypes(w.result.Fields)
	rowCount := len(w.result.RowDatas)
	c := chunk.NewChunkWithCapacity(columns, rowCount)
	writeResultSetDataToTrunk(w.result.Resultset, c)
	return c
}

func (w *weirResultSet) Next(ctx context.Context, c *chunk.Chunk) error {
	// all the data has been converted and set into chunk when calling NewChunk(),
	// so we need to do nothing here
	if w.readed {
		c.Reset()
	}
	w.readed = true
	return nil
}

func (*weirResultSet) StoreFetchedRows(rows []chunk.Row) {
	panic("implement me")
}

func (*weirResultSet) GetFetchedRows() []chunk.Row {
	panic("implement me")
}

func (w *weirResultSet) Close() error {
	atomic.StoreInt32(&w.closed, 1)
	return nil
}
