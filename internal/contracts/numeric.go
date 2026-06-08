// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package contracts

import "math"

// ClampInt32 narrows a non-negative count to the int32 protobuf wire type,
// saturating at the int32 bounds rather than overflowing on the (practically
// unreachable) extremes. It exists so call sites converting counts to proto
// fields stay free of unchecked int -> int32 conversions.
func ClampInt32(value int) int32 {
	if value < 0 {
		return 0
	}

	if value > math.MaxInt32 {
		return math.MaxInt32
	}

	return int32(value)
}
