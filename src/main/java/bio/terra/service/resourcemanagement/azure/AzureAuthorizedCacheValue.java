package bio.terra.service.resourcemanagement.azure;

import java.time.Instant;

public class AzureAuthorizedCacheValue {
  private Instant timeout;
  private String storageAccountKey;

  public AzureAuthorizedCacheValue(Instant timeout, String storageAccountKey) {
    this.timeout = timeout;
    this.storageAccountKey = storageAccountKey;
  }

  public Instant getTimeout() {
    return timeout;
  }

  public String getStorageAccountKey() {
    return storageAccountKey;
  }
}
