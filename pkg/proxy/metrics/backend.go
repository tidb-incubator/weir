package metrics

import "github.com/prometheus/client_golang/prometheus"

const (
	BackendEventIniting = "initing"
	BackendEventInited = "inited"
	BackendEventClosing = "closing"
	BackendEventClosed = "closed"
)

var (
	BackendEventCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: ModuleWeirProxy,
			Subsystem: LabelBackend,
			Name:      "backend_event_total",
			Help:      "Counter of backend event.",
		}, []string{LblNamespace, LblType})

	BackendQueryCounter = prometheus.NewCounterVec(
		prometheus.CounterOpts{
			Namespace: ModuleWeirProxy,
			Subsystem: LabelBackend,
			Name:      "b_conn_cnt",
			Help:      "Counter of backend query count.",
		}, []string{LblNamespace, LblBackendInstance})

	BackendConnInUseGauge = prometheus.NewGaugeVec(
		prometheus.GaugeOpts{
			Namespace: ModuleWeirProxy,
			Subsystem: LabelBackend,
			Name:      "b_conn_in_use",
			Help:      "Number of backend conn in use.",
		}, []string{LblNamespace, LblBackendInstance})
)
