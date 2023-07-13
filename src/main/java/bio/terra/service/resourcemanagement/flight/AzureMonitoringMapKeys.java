package bio.terra.service.resourcemanagement.flight;

public final class AzureMonitoringMapKeys {
  private AzureMonitoringMapKeys() {}

  public static final String PREFIX = "azmonitoring-";
  public static final String LOG_ANALYTICS_WORKSPACE_ID = PREFIX + "law-id";
  public static final String DIAGNOSTIC_SETTING_ID = PREFIX + "ds-id";
  public static final String EXPORT_RULE_ID = PREFIX + "er-id";
  public static final String SENTINEL_ID = PREFIX + "sentinel-id";
  public static final String ALERT_RULE_UNAUTHED_ACCESS_ID = PREFIX + "ar-unauthed-id";
  public static final String NOTIFICATION_RULE_ID = PREFIX + "nr-id";
}
