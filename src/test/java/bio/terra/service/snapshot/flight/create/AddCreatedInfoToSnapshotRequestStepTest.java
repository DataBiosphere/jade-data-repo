package bio.terra.service.snapshot.flight.create;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
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
class AddCreatedInfoToSnapshotRequestStepTest {
  @Mock private SnapshotRequestDao snapshotRequestDao;
  @Mock private FlightContext context;
  private static final UUID SNAPSHOT_REQUEST_ID = UUID.randomUUID();
  private static final UUID CREATED_SNAPSHOT_ID = UUID.randomUUID();
  private static final String SAM_GROUP_NAME = "samGroupName";
  private static final String SAM_GROUP_EMAIL = "samGroupName@firecloud.org";
  private static final String SAM_GROUP_CREATED_BY_EMAIL = "tdr@serviceaccount.com";
  private AddCreatedInfoToSnapshotRequestStep step;
  private FlightMap workingMap;

  @BeforeEach
  void beforeEach() {
    step =
        new AddCreatedInfoToSnapshotRequestStep(
            snapshotRequestDao,
            SNAPSHOT_REQUEST_ID,
            CREATED_SNAPSHOT_ID,
            SAM_GROUP_CREATED_BY_EMAIL);
    workingMap = new FlightMap();
  }

  @Test
  void doStep() throws InterruptedException {
    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_NAME, SAM_GROUP_NAME);
    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_EMAIL, SAM_GROUP_EMAIL);
    when(context.getWorkingMap()).thenReturn(workingMap);
    StepResult result = step.doStep(context);
    verify(snapshotRequestDao)
        .updateCreatedInfo(
            SNAPSHOT_REQUEST_ID,
            CREATED_SNAPSHOT_ID,
            SAM_GROUP_NAME,
            SAM_GROUP_EMAIL,
            SAM_GROUP_CREATED_BY_EMAIL);
    assertEquals(StepResult.getStepResultSuccess(), result);
  }

  @Test
  void doStepFail() {
    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_NAME, SAM_GROUP_NAME);
    when(context.getWorkingMap()).thenReturn(workingMap);
    assertThrows(IllegalArgumentException.class, () -> step.doStep(context));
  }

  @Test
  void undoStep() throws InterruptedException {
    StepResult result = step.undoStep(null);
    verify(snapshotRequestDao).updateCreatedInfo(SNAPSHOT_REQUEST_ID, null, null, null, null);
    assertEquals(StepResult.getStepResultSuccess(), result);
  }
}
