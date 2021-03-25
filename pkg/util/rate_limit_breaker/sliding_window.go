package rate_limit_breaker

import "time"

/*
 * 一个 SlidingWindow 由若干个子单元(Cell)组成，数量(Size)在创建 SlidingWindow 时指定。
 * 各个 Cell 的时长(CellIntervalMs)相同，在创建 SlidingWindow 时指定。
 * Cell 持有一个 map 用于统计计数
 *
 * 自 epoch 以来，按照一个 Cell 的时长，将时间轴切成一个个的段，对应到 Cell。
 * 每个 Cell 记录其开始时间。因为我们并不是按照时间定时更新 Cells，而是 hit 时更新，而 hit 是由调用者控制的。
 */

type Cell struct {
	startMs int64
	// metricName => count
	stats map[string]int64
}

func (cell *Cell) Reset() {
	cell.startMs = 0
	cell.stats = map[string]int64{}
}

type SlidingWindow struct {
	Size           int64
	CellIntervalMs int64
	Cells          []*Cell // invariant: len(Cells) == Size.
}

func NewSlidingWindow(size int64, cellIntervalMs int64) *SlidingWindow {
	cells := make([]*Cell, size)
	for i := 0; int64(i) < size; i++ {
		cells[i] = &Cell{
			startMs: 0,
			stats:   map[string]int64{},
		}
	}

	return &SlidingWindow{
		Size:           size,
		CellIntervalMs: cellIntervalMs,
		Cells:          cells,
	}
}

// 一次动作(如请求)，记为一次 Hit。
func (sw *SlidingWindow) Hit(nowMs int64, metricNames ...string) {
	cell := sw.getCell(nowMs)
	if nowMs-cell.startMs >= sw.CellIntervalMs { // lazily check if cell expired
		cell.startMs = sw.cellStartMs(nowMs)
		cell.stats = map[string]int64{}
	}
	for _, metric := range metricNames {
		cell.stats[metric]++
	}
}

func (sw *SlidingWindow) getCell(nowMs int64) *Cell {
	idx := nowMs / sw.CellIntervalMs % sw.Size
	return sw.Cells[idx]
}

func (sw *SlidingWindow) cellStartMs(nowMs int64) int64 {
	return nowMs - nowMs%sw.CellIntervalMs
}

func (sw *SlidingWindow) GetHits(nowMs int64, metricNames ...string) map[string]int64 {
	windowStart := nowMs - sw.Size*sw.CellIntervalMs
	stats := map[string]int64{}
	for _, cell := range sw.Cells {
		if cell.startMs < windowStart { // lazily check if cell expired
			continue
		}
		for _, metricName := range metricNames {
			stats[metricName] += cell.stats[metricName]
		}
	}
	return stats
}

func (sw *SlidingWindow) GetHit(nowMs int64, metricName string) int64 {
	return sw.GetHits(nowMs, metricName)[metricName]
}

func (sw *SlidingWindow) GetActualDurationMs(nowMs int64) int64 {
	// 当前时间点，可能处在某个 cell 正中间，这里精确计算所涉及 cell 的总时间段
	actualDurationMs := (sw.Size-1)*sw.CellIntervalMs + nowMs%sw.CellIntervalMs
	return actualDurationMs
}

// timestamp in ms
func GetNowMs() int64 {
	return time.Now().UnixNano() / int64(time.Millisecond)
}
