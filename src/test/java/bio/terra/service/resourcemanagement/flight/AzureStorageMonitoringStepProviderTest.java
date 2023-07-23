package bio.terra.service.resourcemanagement.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.service.resourcemanagement.azure.AzureMonitoringService;
import bio.terra.stairway.Step;
import java.util.List;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag("bio.terra.common.category.Unit")
class AzureStorageMonitoringStepProviderTest {

  private static final List<Class<? extends Step>> EXPECTED_STANDARD_STEPS =
      List.of(CreateLogAnalyticsWorkspaceStep.class, CreateDiagnosticSettingStep.class);

  private static final List<Class<? extends Step>> EXPECTED_PROTECTED_DATA_STEPS =
      List.of(
          CreateLogAnalyticsWorkspaceStep.class,
          CreateDiagnosticSettingStep.class,
          CreateExportRuleStep.class,
          CreateSentinelStep.class,
          CreateSentinelAlertRulesStep.class,
          CreateSentinelNotificationRuleStep.class);

  @Mock AzureMonitoringService monitoringService;

  AzureStorageMonitoringStepProvider stepProvider;

  @BeforeEach
  void setUp() {
    stepProvider = new AzureStorageMonitoringStepProvider(monitoringService);
  }

  @Test
  void testConfigureStandardSteps() {
    assertThat(
        "only standard steps returned",
        stepProvider.configureSteps(false).stream().map(s -> s.step().getClass()).toList(),
        is(EXPECTED_STANDARD_STEPS));
  }

  @Test
  void testConfigureProtectedDataSteps() {
    assertThat(
        "protected data steps also returned",
        stepProvider.configureSteps(true).stream().map(s -> s.step().getClass()).toList(),
        is(EXPECTED_PROTECTED_DATA_STEPS));
  }
}