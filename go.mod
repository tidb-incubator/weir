module github.com/tidb-incubator/weir

go 1.14

require (
	github.com/gin-gonic/gin v1.7.2
	github.com/go-playground/validator/v10 v10.8.0 // indirect
	github.com/goccy/go-yaml v1.8.2
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/json-iterator/go v1.1.11 // indirect
	github.com/mattn/go-isatty v0.0.13 // indirect
	github.com/opentracing/opentracing-go v1.1.0
	github.com/pingcap/check v0.0.0-20200212061837-5e12011dc712
	github.com/pingcap/errors v0.11.5-0.20190809092503-95897b64e011
	github.com/pingcap/failpoint v0.0.0-20200702092429-9f69995143ce
	github.com/pingcap/parser v0.0.0-20200803072748-fdf66528323d
	github.com/pingcap/tidb v1.1.0-beta.0.20200826081922-9c1c21270001
	github.com/prometheus/client_golang v1.5.1
	github.com/shirou/gopsutil v3.21.6+incompatible // indirect
	github.com/siddontang/go v0.0.0-20180604090527-bdc77568d726
	github.com/siddontang/go-mysql v1.1.0
	github.com/stretchr/testify v1.6.1
	github.com/tklauser/go-sysconf v0.3.7 // indirect
	github.com/ugorji/go v1.2.6 // indirect
	go.etcd.io/etcd v0.5.0-alpha.5.0.20191023171146-3cf2f69b5738
	go.uber.org/zap v1.15.0
	golang.org/x/net v0.0.0-20210226172049-e18ecbb05110
	google.golang.org/protobuf v1.27.1 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
)

replace github.com/siddontang/go-mysql => github.com/ibanyu/go-mysql v1.1.0
