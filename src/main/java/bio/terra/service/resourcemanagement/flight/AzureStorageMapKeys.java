package bio.terra.service.resourcemanagement.flight;

public final class AzureStorageMapKeys {
  private AzureStorageMapKeys() {}

  public static final String PREFIX = "azstorage-";
  public static final String SUBSCRIPTION_ID = PREFIX + "subscription-id";
  public static final String RESOURCE_GROUP_NAME = PREFIX + "resource-group-name";
  public static final String STORAGE_ACCOUNT_NAME = PREFIX + "storage-account-name";
  public static final String ERROR_COLLECTOR = PREFIX + "error-collector";
}
