package bio.terra.service.auth.iam;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.commons.lang3.StringUtils;

// NOTE: these action enums must have exactly the same text as the Sam action name.
public enum IamAction {
  // common
  CREATE,
  DELETE,
  READ_POLICY,
  READ_POLICIES,
  ALTER_POLICIES,
  UPDATE_PASSPORT_IDENTIFIER,
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
    this.samActionName = name().toLowerCase();
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
