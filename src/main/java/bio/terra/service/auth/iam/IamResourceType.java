package bio.terra.service.auth.iam;

import bio.terra.model.IamResourceTypeEnum;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.commons.lang3.StringUtils;

public enum IamResourceType {
  // To add a hyphenated resource type, special case it in fromEnum and toIamResourceTypeEnum
  // since the swagger generated enums will use _
  DATAREPO("datarepo"),
  DATASET("dataset"),
  DATASNAPSHOT("datasnapshot"),
  SPEND_PROFILE("spend-profile"),
  SNAPSHOT_BUILDER_REQUEST("snapshot-builder-request"),
  WORKSPACE("workspace");

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

  public static IamResourceType fromEnum(IamResourceTypeEnum apiEnum) {
    if (apiEnum == IamResourceTypeEnum.SPEND_PROFILE) {
      return IamResourceType.SPEND_PROFILE;
    } else if (apiEnum == IamResourceTypeEnum.SNAPSHOT_BUILDER_REQUEST) {
      return IamResourceType.SNAPSHOT_BUILDER_REQUEST;
    }
    return fromString(apiEnum.toString());
  }

  public static IamResourceType fromString(String stringResourceType) {
    for (IamResourceType b : IamResourceType.values()) {
      if (b.getSamResourceName().equalsIgnoreCase(stringResourceType)) {
        return b;
      }
    }
    throw new RuntimeException("Invalid resource type: " + stringResourceType);
  }

  public static IamResourceTypeEnum toIamResourceTypeEnum(IamResourceType resourceType) {
    if (resourceType.equals(IamResourceType.SPEND_PROFILE)) {
      return IamResourceTypeEnum.SPEND_PROFILE;
    } else if (resourceType.equals(IamResourceType.SNAPSHOT_BUILDER_REQUEST)) {
      return IamResourceTypeEnum.SNAPSHOT_BUILDER_REQUEST;
    }
    for (IamResourceTypeEnum b : IamResourceTypeEnum.values()) {
      if (String.valueOf(b).equalsIgnoreCase(resourceType.toString())) {
        return b;
      }
    }
    throw new RuntimeException("Invalid resource type: " + resourceType);
  }
}
