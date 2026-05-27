// SPDX-FileCopyrightText: 2026 Blackcat Informatics Inc.
// SPDX-License-Identifier: AGPL-3.0-only

package cli

import (
	"fmt"
	"strings"

	"blackcat.ca/gmeow/internal/config"
)

func requireInstanceConfirmation(
	loaded *config.Loaded,
	operation string,
	confirmation string,
) error {
	if loaded == nil {
		return fmt.Errorf("%s requires loaded config", operation)
	}

	instanceID := strings.TrimSpace(loaded.Config.System.InstanceID)
	if !productionLikeInstance(loaded) {
		return nil
	}

	if confirmation != instanceID {
		return fmt.Errorf(
			"%s refused for instance %q; rerun with --confirm-instance %s",
			operation,
			instanceID,
			instanceID,
		)
	}

	return nil
}

func productionLikeInstance(loaded *config.Loaded) bool {
	instanceID := strings.TrimSpace(loaded.Config.System.InstanceID)
	if instanceID == "" || instanceID == "test" || strings.HasPrefix(instanceID, "test-") {
		return false
	}

	if loaded.Resolved.Scheduler.QueuePrefix == "gmeow.test." {
		return false
	}

	return true
}
