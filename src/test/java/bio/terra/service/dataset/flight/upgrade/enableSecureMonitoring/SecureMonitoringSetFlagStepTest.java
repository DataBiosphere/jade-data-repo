package bio.terra.service.dataset.flight.upgrade.enableSecureMonitoring;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SecureMonitoringSetFlagStepTest {
  @Mock private DatasetDao datasetDao;
  @Mock private FlightContext context;
  private SecureMonitoringSetFlagStep step;
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private UUID datasetId;
  private FlightMap workingMap;

  @BeforeEach
  void setUp() {
    datasetId = UUID.randomUUID();
    step = new SecureMonitoringSetFlagStep(datasetDao, TEST_USER, true);
    workingMap = new FlightMap();
    workingMap.put(DatasetWorkingMapKeys.DATASET_ID, datasetId);
    when(context.getWorkingMap()).thenReturn(workingMap);
  }

  @Test
  void doStep() throws InterruptedException {
    when(datasetDao.setSecureMonitoring(datasetId, true, TEST_USER)).thenReturn(true);
    StepResult result = step.doStep(context);
    assertEquals(StepResult.getStepResultSuccess(), result);
  }

  @Test
  void undoStep() throws InterruptedException {
    workingMap.put(DatasetWorkingMapKeys.SECURE_MONITORING_ENABLED, false);
    when(datasetDao.setSecureMonitoring(datasetId, false, TEST_USER)).thenReturn(true);
    StepResult result = step.undoStep(context);
    assertEquals(StepResult.getStepResultSuccess(), result);
  }
}
