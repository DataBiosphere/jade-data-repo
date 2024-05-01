package bio.terra.service.auth.iam;

import bio.terra.model.IamResourceTypeEnum;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import org.apache.commons.lang3.StringUtils;

public enum IamResourceType {
  DATAREPO("datarepo", IamResourceTypeEnum.DATAREPO),
  DATASET("dataset", IamResourceTypeEnum.DATASET),
  DATASNAPSHOT("datasnapshot", IamResourceTypeEnum.DATASNAPSHOT),
  SPEND_PROFILE("spend-profile", IamResourceTypeEnum.SPEND_PROFILE),
  SNAPSHOT_BUILDER_REQUEST(
      "snapshot-builder-request", IamResourceTypeEnum.SNAPSHOT_BUILDER_REQUEST),
  WORKSPACE("workspace", IamResourceTypeEnum.WORKSPACE);

  private final String samResourceName;
  private final IamResourceTypeEnum iamResourceTypeEnum;

  IamResourceType(String samResourceName, IamResourceTypeEnum iamResourceTypeEnum) {
    this.samResourceName = samResourceName;
    this.iamResourceTypeEnum = iamResourceTypeEnum;
  }

  public String getSamResourceName() {
    return samResourceName;
  }

  public IamResourceTypeEnum getIamResourceTypeEnum() {
    return iamResourceTypeEnum;
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
    for (IamResourceType b : IamResourceType.values()) {
      if (b.getIamResourceTypeEnum().equals(apiEnum)) {
        return b;
      }
    }
    return null;
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
    return resourceType.getIamResourceTypeEnum();
  }
}
