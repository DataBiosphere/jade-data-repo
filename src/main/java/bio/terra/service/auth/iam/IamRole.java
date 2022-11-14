package bio.terra.service.auth.iam;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.commons.lang3.StringUtils;

public enum IamRole {
  ADMIN,
  MEMBER,
  STEWARD,
  CUSTODIAN,
  READER,
  DISCOVERER,
  OWNER,
  USER,
  SNAPSHOT_CREATOR;

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
