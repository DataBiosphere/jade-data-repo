package bio.terra.service.snapshot.flight.create;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamNotFoundException;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@Tag(Unit.TAG)
@ExtendWith(MockitoExtension.class)
class CreateSnapshotFirecloudGroupNameStepTest {
  @Mock private IamService iamService;
  @Mock private FlightContext flightContext;
  private final UUID snapshotId = UUID.randomUUID();

  private CreateSnapshotFirecloudGroupNameStep step;

  @BeforeEach
  void setUp() {
    step = new CreateSnapshotFirecloudGroupNameStep(snapshotId, iamService);
  }

  @Test
  void doStep() throws InterruptedException {
    FlightMap workingMap = new FlightMap();
    String groupName = IamService.constructFirecloudGroupName(String.valueOf(snapshotId));
    when(iamService.getGroup(startsWith(groupName)))
        .thenThrow(new IamNotFoundException(new Throwable("Group not found")));
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    assertEquals(StepResult.getStepResultSuccess(), step.doStep(flightContext));
    assertEquals(
        workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_NAME, String.class),
        groupName);
  }

  @Test
  void doStepFailure() throws InterruptedException {
    assertEquals(step.doStep(flightContext).getStepStatus(), StepStatus.STEP_RESULT_FAILURE_FATAL);
    verifyNoInteractions(flightContext);
  }

  @Test
  void undoStep() {
    assertEquals(step.undoStep(flightContext), StepResult.getStepResultSuccess());
  }
}
