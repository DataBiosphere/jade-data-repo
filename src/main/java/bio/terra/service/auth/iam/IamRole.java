package bio.terra.service.auth.iam;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.commons.lang3.StringUtils;

public enum IamRole {
  ADMIN,
  STEWARD,
  CUSTODIAN,
  AGGREGATE_DATA_READER,
  READER,
  DISCOVERER,
  SNAPSHOT_BUILDER_MANAGER,
  OWNER,
  USER,
  APPROVER,
  SNAPSHOT_CREATOR,
  /** A role used by Firecloud Managed Groups */
  MEMBER;

  @Override
  @JsonValue
  public String toString() {
    return StringUtils.lowerCase(name());
  }

  @JsonCreator
  public static IamRole fromValue(String text) {
    for (IamRole b : IamRole.values()) {
      if (StringUtils.equalsIgnoreCase(b.name(), text)) {
        return b;
      }
    }
    return null;
  }
}
