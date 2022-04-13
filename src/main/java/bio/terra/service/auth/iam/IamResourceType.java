package bio.terra.service.auth.iam;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.commons.lang3.StringUtils;

public enum IamResourceType {
  DATAREPO("datarepo"),
  DATASET("dataset"),
  DATASNAPSHOT("datasnapshot"),
  SPEND_PROFILE("spend-profile");

  private final String samResourceName;

  IamResourceType(String samResourceName) {
    this.samResourceName = samResourceName;
  }

  public String getSamResourceName() {
    return samResourceName;
  }

  @Override
  @JsonValue
  public String toString() {
    return samResourceName;
  }

  @JsonCreator
  static IamResourceType fromValue(String text) {
    for (IamResourceType b : IamResourceType.values()) {
      if (StringUtils.equalsIgnoreCase(b.getSamResourceName(), text)) {
        return b;
      }
    }
    return null;
  }
}
