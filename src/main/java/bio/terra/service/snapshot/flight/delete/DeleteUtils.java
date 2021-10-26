package bio.terra.service.snapshot.flight.delete;

import bio.terra.common.CloudPlatformWrapper;

public class DeleteUtils {

  public static boolean performGCPDeleteStep(CloudPlatformWrapper platform) {
    if (platform == null || platform.isGcp()) {
      return true;
    }
    return false;
  }
}
