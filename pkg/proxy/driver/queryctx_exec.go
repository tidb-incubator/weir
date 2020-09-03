package driver

import (
	"context"

	"github.com/pingcap-incubator/weir/pkg/proxy/server"
)

func (q *QueryCtxImpl) doExecute(ctx context.Context, sql string) ([]server.ResultSet, error) {
	conn, err := q.backend.GetConn(ctx)
	if err != nil {
		return nil, err
	}

	defer q.backend.PutConn(ctx, conn)

	result, err := conn.Execute(sql)
	if err != nil {
		return nil, err
	}

	resultSet := wrapMySQLResult(result)
	return []server.ResultSet{resultSet}, nil
}
