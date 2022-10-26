package bio.terra.common;

import bio.terra.model.IamResourceTypeEnum;
import bio.terra.service.auth.iam.IamResourceType;

/**
 *   The swagger codegen we use has issues with ENUM values with hyphens.
 *   This utility helps convert between the internally used and externally
 *   visible ENUMS that represent the IamResourceType.
 */
public class IamResourceTypeCODEC {
  private static final String MAGIC_SPEND_STRING = "spendprofile";

  public static IamResourceType toIamResourceType(String stringResourceType) {
    if (stringResourceType.equalsIgnoreCase(MAGIC_SPEND_STRING)) {
      return IamResourceType.SPEND_PROFILE;
    }
    for (IamResourceType b : IamResourceType.values()) {
      if (String.valueOf(b).equalsIgnoreCase(stringResourceType)) {
        return b;
      }
    }
    throw new RuntimeException("Invalid resource type: " + stringResourceType);
  }

  public static IamResourceTypeEnum toIamResourceTypeEnum(IamResourceType resourceType) {
    if (resourceType.equals(IamResourceType.SPEND_PROFILE)) {
      return IamResourceTypeEnum.SPENDPROFILE;
    }
    for (IamResourceTypeEnum b : IamResourceTypeEnum.values()) {
      if (String.valueOf(b).equalsIgnoreCase(resourceType.toString())) {
        return b;
      }
    }
    throw new RuntimeException("Invalid resource type: " + resourceType);
  }
}
