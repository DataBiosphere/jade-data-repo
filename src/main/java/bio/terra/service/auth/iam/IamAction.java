package bio.terra.service.auth.iam;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;

// By default these action enums have exactly the same name as the Sam action name.
// However, we also support an override where we are unable to match the enum to the Sam action,
// necessary for Sam actions with enum-incompatible characters (e.g. colons).
public enum IamAction {
  // common
  CREATE,
  DELETE,
  READ_POLICY,
  READ_POLICIES,
  SHARE_POLICY_READER("share_policy::reader"),
  ALTER_POLICIES,
  UPDATE_PASSPORT_IDENTIFIER,
  UPDATE_AUTH_DOMAIN,
  // datarepo (admin-only actions)
  LIST_JOBS,
  DELETE_JOBS,
  CONFIGURE,
  REGISTER_DRS_ALIASES,
  RUN_UPGRADE_FLIGHT,
  SYNC_DUOS_USERS,
  // dataset
  MANAGE_SCHEMA,
  READ_DATASET,
  INGEST_DATA,
  SOFT_DELETE,
  HARD_DELETE,
  LINK_SNAPSHOT,
  UNLINK_SNAPSHOT,
  VIEW_SNAPSHOT_BUILDER_SETTINGS,
  UPDATE_SNAPSHOT_BUILDER_SETTINGS,
  // snapshots
  UPDATE_SNAPSHOT,
  READ_DATA,
  // journal
  VIEW_JOURNAL,
  DISCOVER_DATA,
  EXPORT_SNAPSHOT,
  // billing profiles
  UPDATE_BILLING_ACCOUNT,
  LINK,
  LOCK_RESOURCE,
  UNLOCK_RESOURCE;

  private final String samActionName;

  IamAction() {
    this.samActionName = name().toLowerCase();
  }

  IamAction(String samActionName) {
    this.samActionName = samActionName;
  }

  @Override
  @JsonValue
  public String toString() {
    return samActionName;
  }

  @JsonCreator
  public static IamAction fromValue(String text) {
    for (IamAction b : IamAction.values()) {
      if (b.samActionName.equalsIgnoreCase(text)) {
        return b;
      }
    }
    return null;
  }
}
