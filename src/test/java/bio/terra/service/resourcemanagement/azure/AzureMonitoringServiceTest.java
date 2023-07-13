package bio.terra.service.resourcemanagement.azure;

import static bio.terra.service.filedata.azure.util.AzureConstants.NOT_FOUND_CODE;
import static bio.terra.service.filedata.azure.util.AzureConstants.RESOURCE_NOT_FOUND_CODE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.resourcemanagement.MetadataDataAccessUtils;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration.Credentials;
import com.azure.core.http.HttpResponse;
import com.azure.core.management.exception.ManagementError;
import com.azure.core.management.exception.ManagementException;
import com.azure.resourcemanager.AzureResourceManager;
import com.azure.resourcemanager.loganalytics.LogAnalyticsManager;
import com.azure.resourcemanager.loganalytics.models.DataExport;
import com.azure.resourcemanager.loganalytics.models.DataExports;
import com.azure.resourcemanager.loganalytics.models.Workspace;
import com.azure.resourcemanager.loganalytics.models.Workspaces;
import com.azure.resourcemanager.monitor.models.DiagnosticSetting;
import com.azure.resourcemanager.monitor.models.DiagnosticSettings;
import com.azure.resourcemanager.securityinsights.SecurityInsightsManager;
import com.azure.resourcemanager.securityinsights.models.AlertRule;
import com.azure.resourcemanager.securityinsights.models.AlertRules;
import com.azure.resourcemanager.securityinsights.models.AutomationRule;
import com.azure.resourcemanager.securityinsights.models.AutomationRules;
import com.azure.resourcemanager.securityinsights.models.SentinelOnboardingState;
import com.azure.resourcemanager.securityinsights.models.SentinelOnboardingStates;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag("bio.terra.common.category.Unit")
class AzureMonitoringServiceTest {

  @Mock private AzureResourceConfiguration resourceConfiguration;

  // Azure clients
  @Mock private LogAnalyticsManager logAnalyticsManager;
  @Mock private AzureResourceManager azureResourceManager;
  @Mock private SecurityInsightsManager securityInsightsManager;

  private AzureMonitoringService service;

  private static final UUID HOME_TENANT_ID = UUID.randomUUID();
  private static final UUID SUBSCRIPTION_ID = UUID.randomUUID();
  private static final BillingProfileModel PROFILE_MODEL =
      new BillingProfileModel().subscriptionId(SUBSCRIPTION_ID);

  private static final String STORAGE_ACCOUNT_NAME = "foo";
  private static final String TOP_LEVEL_CONTAINER = "tlc";
  private static final String RESOURCE_GROUP = "rg";
  private static final String APPLICATION_NAME = "myapp";
  private static final AzureApplicationDeploymentResource APPLICATION =
      new AzureApplicationDeploymentResource()
          .azureResourceGroupName(RESOURCE_GROUP)
          .azureApplicationDeploymentId(
              MetadataDataAccessUtils.getApplicationDeploymentId(
                  SUBSCRIPTION_ID, RESOURCE_GROUP, APPLICATION_NAME));

  private static final AzureStorageAccountResource STORAGE_ACCOUNT =
      new AzureStorageAccountResource()
          .name(STORAGE_ACCOUNT_NAME)
          .topLevelContainer(TOP_LEVEL_CONTAINER)
          .applicationResource(APPLICATION);

  @BeforeEach
  public void setUp() throws Exception {
    service = new AzureMonitoringService(resourceConfiguration);
  }

  @Test
  void testGetLogAnalytics() {
    mockLogAnalyticsClient();
    Workspaces workspaceClient = mock(Workspaces.class);
    when(logAnalyticsManager.workspaces()).thenReturn(workspaceClient);
    Workspace workspace = mock(Workspace.class);
    when(workspaceClient.getByResourceGroup(RESOURCE_GROUP, STORAGE_ACCOUNT_NAME))
        .thenReturn(workspace);
    assertThat(
        "value returned when found",
        service.getLogAnalyticsWorkspace(PROFILE_MODEL, STORAGE_ACCOUNT),
        is(workspace));
  }

  @Test
  void testGetLogAnalyticsNotFound() {
    mockLogAnalyticsClient();
    Workspaces workspaceClient = mock(Workspaces.class);
    when(logAnalyticsManager.workspaces()).thenReturn(workspaceClient);
    when(workspaceClient.getByResourceGroup(RESOURCE_GROUP, STORAGE_ACCOUNT_NAME))
        .thenThrow(
            new ManagementException(
                "Not found", null, new ManagementError(RESOURCE_NOT_FOUND_CODE, null)));
    assertThat(
        "null returned when not found",
        service.getLogAnalyticsWorkspace(PROFILE_MODEL, STORAGE_ACCOUNT),
        is(nullValue()));
  }

  @Test
  void testGetDiagnosticSetting() {
    mockArmClient();
    DiagnosticSettings diagnosticSettingsClient = mock(DiagnosticSettings.class);
    when(azureResourceManager.diagnosticSettings()).thenReturn(diagnosticSettingsClient);
    DiagnosticSetting diagnosticSetting = mock(DiagnosticSetting.class);
    when(diagnosticSettingsClient.get(
            STORAGE_ACCOUNT.getStorageAccountId() + "/blobServices/default", STORAGE_ACCOUNT_NAME))
        .thenReturn(diagnosticSetting);
    assertThat(
        "value returned when found",
        service.getDiagnosticSetting(PROFILE_MODEL, STORAGE_ACCOUNT),
        is(diagnosticSetting));
  }

  @Test
  void testGetDiagnosticSettingNotFound() {
    mockArmClient();
    DiagnosticSettings diagnosticSettingsClient = mock(DiagnosticSettings.class);
    when(azureResourceManager.diagnosticSettings()).thenReturn(diagnosticSettingsClient);
    when(diagnosticSettingsClient.get(
            STORAGE_ACCOUNT.getStorageAccountId() + "/blobServices/default", STORAGE_ACCOUNT_NAME))
        .thenThrow(
            new ManagementException(
                "Not found", null, new ManagementError(RESOURCE_NOT_FOUND_CODE, null)));
    assertThat(
        "null returned when not found",
        service.getDiagnosticSetting(PROFILE_MODEL, STORAGE_ACCOUNT),
        is(nullValue()));
  }

  @Test
  void testGetLogAnalyticsExportRule() {
    mockLogAnalyticsClient();
    DataExports dataExportClient = mock(DataExports.class);
    when(logAnalyticsManager.dataExports()).thenReturn(dataExportClient);
    DataExport dataExportRule = mock(DataExport.class);
    when(dataExportClient.get(RESOURCE_GROUP, STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_NAME))
        .thenReturn(dataExportRule);
    assertThat(
        "value returned when found",
        service.getDataExportRule(PROFILE_MODEL, STORAGE_ACCOUNT),
        is(dataExportRule));
  }

  @Test
  void testGetLogAnalyticsExportRuleNotFound() {
    mockLogAnalyticsClient();
    DataExports dataExportClient = mock(DataExports.class);
    when(logAnalyticsManager.dataExports()).thenReturn(dataExportClient);

    HttpResponse response = mock(HttpResponse.class);
    when(response.getStatusCode()).thenReturn(404);

    when(dataExportClient.get(RESOURCE_GROUP, STORAGE_ACCOUNT_NAME, STORAGE_ACCOUNT_NAME))
        .thenThrow(new ManagementException("Not found", response, null));
    assertThat(
        "null returned when not found",
        service.getDataExportRule(PROFILE_MODEL, STORAGE_ACCOUNT),
        is(nullValue()));
  }

  @Test
  void testGetSentinel() {
    mockSecurityInsightsClient();
    SentinelOnboardingStates sentinelClient = mock(SentinelOnboardingStates.class);
    when(securityInsightsManager.sentinelOnboardingStates()).thenReturn(sentinelClient);
    SentinelOnboardingState sentinel = mock(SentinelOnboardingState.class);
    when(sentinelClient.get(RESOURCE_GROUP, STORAGE_ACCOUNT_NAME, "default")).thenReturn(sentinel);
    assertThat(
        "value returned when found",
        service.getSentinel(PROFILE_MODEL, STORAGE_ACCOUNT),
        is(sentinel));
  }

  @Test
  void testGetSentinelNotFound() {
    mockSecurityInsightsClient();
    SentinelOnboardingStates sentinelClient = mock(SentinelOnboardingStates.class);
    when(securityInsightsManager.sentinelOnboardingStates()).thenReturn(sentinelClient);
    when(sentinelClient.get(RESOURCE_GROUP, STORAGE_ACCOUNT_NAME, "default"))
        .thenThrow(
            new ManagementException("Not found", null, new ManagementError(NOT_FOUND_CODE, null)));
    assertThat(
        "null returned when not found",
        service.getSentinel(PROFILE_MODEL, STORAGE_ACCOUNT),
        is(nullValue()));
  }

  @Test
  void testGetSentinelAlertRule() {
    mockSecurityInsightsClient();
    AlertRules alertRulesClient = mock(AlertRules.class);
    when(securityInsightsManager.alertRules()).thenReturn(alertRulesClient);
    AlertRule alertRule = mock(AlertRule.class);
    when(alertRulesClient.get(RESOURCE_GROUP, STORAGE_ACCOUNT_NAME, "UnauthorizedAccess"))
        .thenReturn(alertRule);
    assertThat(
        "value returned when found",
        service.getSentinelRuleUnauthorizedAccess(PROFILE_MODEL, STORAGE_ACCOUNT),
        is(alertRule));
  }

  @Test
  void testGetSentinelAlertRuleNotFound() {
    mockSecurityInsightsClient();
    AlertRules alertRulesClient = mock(AlertRules.class);
    when(securityInsightsManager.alertRules()).thenReturn(alertRulesClient);
    when(alertRulesClient.get(RESOURCE_GROUP, STORAGE_ACCOUNT_NAME, "UnauthorizedAccess"))
        .thenThrow(
            new ManagementException("Not found", null, new ManagementError(NOT_FOUND_CODE, null)));
    assertThat(
        "null returned when not found",
        service.getSentinelRuleUnauthorizedAccess(PROFILE_MODEL, STORAGE_ACCOUNT),
        is(nullValue()));
  }

  @Test
  void testNotificationRule() {
    mockSecurityInsightsClient();
    AutomationRules automationRulesClient = mock(AutomationRules.class);
    when(securityInsightsManager.automationRules()).thenReturn(automationRulesClient);
    AutomationRule automationRule = mock(AutomationRule.class);
    when(automationRulesClient.get(
            RESOURCE_GROUP, STORAGE_ACCOUNT_NAME, "runSendSlackNotificationPlaybook"))
        .thenReturn(automationRule);
    assertThat(
        "value returned when found",
        service.getNotificationRule(PROFILE_MODEL, STORAGE_ACCOUNT),
        is(automationRule));
  }

  @Test
  void testNotificationRuleNotFound() {
    mockSecurityInsightsClient();
    AutomationRules automationRulesClient = mock(AutomationRules.class);
    when(securityInsightsManager.automationRules()).thenReturn(automationRulesClient);
    when(automationRulesClient.get(
            RESOURCE_GROUP, STORAGE_ACCOUNT_NAME, "runSendSlackNotificationPlaybook"))
        .thenThrow(
            new ManagementException("Not found", null, new ManagementError(NOT_FOUND_CODE, null)));
    assertThat(
        "null returned when not found",
        service.getNotificationRule(PROFILE_MODEL, STORAGE_ACCOUNT),
        is(nullValue()));
  }

  private void mockLogAnalyticsClient() {
    Credentials credentials = new Credentials();
    credentials.setHomeTenantId(HOME_TENANT_ID);
    when(resourceConfiguration.getCredentials()).thenReturn(credentials);

    when(resourceConfiguration.getLogAnalyticsManagerClient(HOME_TENANT_ID, SUBSCRIPTION_ID))
        .thenReturn(logAnalyticsManager);
  }

  private void mockSecurityInsightsClient() {
    Credentials credentials = new Credentials();
    credentials.setHomeTenantId(HOME_TENANT_ID);
    when(resourceConfiguration.getCredentials()).thenReturn(credentials);

    when(resourceConfiguration.getSecurityInsightsManagerClient(HOME_TENANT_ID, SUBSCRIPTION_ID))
        .thenReturn(securityInsightsManager);
  }

  private void mockArmClient() {
    when(resourceConfiguration.getClient(SUBSCRIPTION_ID)).thenReturn(azureResourceManager);
  }
}
