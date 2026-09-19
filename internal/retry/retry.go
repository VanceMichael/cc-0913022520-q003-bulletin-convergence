// Package retry 提供分级退避计算：第 attempt 次失败（1 起计）后的等待时长
// 取 tiers[min(attempt-1, len(tiers)-1)]，即层级用完后保持最后一级。
package retry

import "time"

func Delay(tiers []time.Duration, attempt int) time.Duration {
	if len(tiers) == 0 {
		return 0
	}
	if attempt < 1 {
		attempt = 1
	}
	idx := attempt - 1
	if idx > len(tiers)-1 {
		idx = len(tiers) - 1
	}
	return tiers[idx]
}
