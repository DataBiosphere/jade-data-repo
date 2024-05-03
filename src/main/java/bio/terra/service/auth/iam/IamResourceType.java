package bio.terra.service.auth.iam;

import bio.terra.model.IamResourceTypeEnum;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonValue;
import java.util.Arrays;
import java.util.Objects;

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

  @Override
  @JsonValue
  public String toString() {
    return samResourceName;
  }

  @FunctionalInterface
  private interface FilterFunction {
    boolean apply(IamResourceType resourceType);
  }

  private static IamResourceType findIamResourceType(FilterFunction filter) {
    return Arrays.stream(values()).filter(filter::apply).findFirst().orElse(null);
  }

  @JsonCreator
  static IamResourceType fromValue(String text) {
    return findIamResourceType(
        iamResourceType -> iamResourceType.getSamResourceName().equalsIgnoreCase(text));
  }

  public static IamResourceType fromEnum(IamResourceTypeEnum apiEnum) {
    return Objects.requireNonNull(
        findIamResourceType(iamResourceType -> iamResourceType.iamResourceTypeEnum == apiEnum),
        () -> String.format("Invalid resource type: %s", apiEnum.toString()));
  }

  public static IamResourceType fromString(String stringResourceType) {
    return Objects.requireNonNull(
        findIamResourceType(
            iamResourceType ->
                iamResourceType.getSamResourceName().equalsIgnoreCase(stringResourceType)),
        () -> String.format("Invalid resource type: %s", stringResourceType));
  }

  public static IamResourceTypeEnum toIamResourceTypeEnum(IamResourceType resourceType) {
    return resourceType.iamResourceTypeEnum;
  }
}
