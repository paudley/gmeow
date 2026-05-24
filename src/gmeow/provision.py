# SPDX-FileCopyrightText: 2026 Blackcat Informatics® Inc.
# SPDX-License-Identifier: MIT
"""Bootstrap helpers for local Gmeow installations.

The provisioning helpers manifest OAuth client/service-account files, ensure data directories
exist, and verify Gmail scopes are present. They keep the operator-facing setup steps out of the
runtime path so daemons can assume a sane environment.
"""

from dataclasses import dataclass
from pathlib import Path

from .config import GMAIL_MODIFY_SCOPE


@dataclass(frozen=True, slots=True)
class ProvisionPlan:
    """Represent ProvisionPlan data and behavior."""

    commands: list[list[str]]
    admin_steps: list[str]

    def shell_script(self) -> str:
        """Shell script."""
        return "\n".join(" ".join(command) for command in self.commands)


def build_gcloud_provision_plan(project_id: str, service_account_name: str, key_file: Path) -> ProvisionPlan:
    """Build gcloud provision plan."""
    email = f"{service_account_name}@{project_id}.iam.gserviceaccount.com"
    return ProvisionPlan(
        commands=[
            ["gcloud", "config", "set", "project", project_id],
            ["gcloud", "services", "enable", "gmail.googleapis.com", "admin.googleapis.com", "iamcredentials.googleapis.com"],
            ["gcloud", "iam", "service-accounts", "create", service_account_name, "--display-name", "Gmeow Gmail Delegation"],
            ["gcloud", "iam", "service-accounts", "keys", "create", str(key_file), "--iam-account", email],
            ["gcloud", "iam", "service-accounts", "describe", email, "--format", "value(oauth2ClientId)"],
        ],
        admin_steps=[
            "Copy the oauth2ClientId printed by the final gcloud command.",
            "In Google Admin Console, open Security > API Controls > Domain-wide Delegation.",
            f"Authorize the client ID with this scope: {GMAIL_MODIFY_SCOPE}",
            "Set `subject` in the [gmeow] table in config.toml to the Workspace mailbox to access.",
        ],
    )
