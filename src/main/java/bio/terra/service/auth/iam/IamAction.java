package bio.terra.service.auth.iam;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.commons.lang3.StringUtils;

public enum IamAction {
  // common
  CREATE,
  DELETE,
  SHARE_POLICY_STEWARD("share_policy::steward"),
  READ_POLICY,
  READ_POLICIES,
  ALTER_POLICIES,
  // datarepo
  LIST_JOBS,
  DELETE_JOBS,
  CONFIGURE,
  // dataset
  MANAGE_SCHEMA,
  READ_DATASET,
  INGEST_DATA,
  SOFT_DELETE,
  HARD_DELETE,
  LINK_SNAPSHOT,
  UNLINK_SNAPSHOT,
  // snapshots,
  UPDATE_SNAPSHOT,
  READ_DATA,
  DISCOVER_DATA,
  // billing profiles
  UPDATE_BILLING_ACCOUNT,
  LINK;

  private final String samActionName;

  IamAction() {
    this.samActionName = StringUtils.lowerCase(name());
  }

  /**
   * When the enum name's text does not exactly match that of the Sam action name, specify it during
   * construction.
   *
   * @param samActionName
   */
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
      if (b.name().equals(StringUtils.upperCase(text))) {
        return b;
      }
    }
    return null;
  }
}
