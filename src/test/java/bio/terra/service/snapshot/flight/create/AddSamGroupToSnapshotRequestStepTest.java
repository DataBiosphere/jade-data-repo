package bio.terra.service.snapshot.flight.create;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
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
class AddSamGroupToSnapshotRequestStepTest {

  @Mock private SnapshotRequestDao snapshotRequestDao;
  @Mock private FlightContext flightContext;
  private static final UUID SNAPSHOT_REQUEST_ID = UUID.randomUUID();
  private static final String SAM_GROUP_NAME = "samGroupName";
  private static final String SAM_GROUP_EMAIL = "samGroupName@firecloud.org";
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private AddSamGroupToSnapshotRequestStep step;
  private FlightMap workingMap;

  @BeforeEach
  void setUp() {
    step = new AddSamGroupToSnapshotRequestStep(snapshotRequestDao, SNAPSHOT_REQUEST_ID, TEST_USER);
    workingMap = new FlightMap();
  }

  @Test
  void doStep() throws InterruptedException {
    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_NAME, SAM_GROUP_NAME);
    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_EMAIL, SAM_GROUP_EMAIL);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    StepResult result = step.doStep(flightContext);
    verify(snapshotRequestDao)
        .updateSamGroup(SNAPSHOT_REQUEST_ID, SAM_GROUP_NAME, SAM_GROUP_EMAIL, TEST_USER.getEmail());
    assertEquals(StepResult.getStepResultSuccess(), result);
  }

  @Test
  void doStepFail() {
    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_NAME, SAM_GROUP_NAME);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    assertThrows(IllegalArgumentException.class, () -> step.doStep(flightContext));
  }

  @Test
  void undoStep() throws InterruptedException {
    StepResult result = step.undoStep(flightContext);
    verify(snapshotRequestDao).updateSamGroup(SNAPSHOT_REQUEST_ID, null, null, null);
    assertEquals(StepResult.getStepResultSuccess(), result);
  }
}
