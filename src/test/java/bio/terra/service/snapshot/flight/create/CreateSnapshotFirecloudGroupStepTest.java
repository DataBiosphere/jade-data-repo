package bio.terra.service.snapshot.flight.create;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamConflictException;
import bio.terra.service.duos.DuosService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
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
class CreateSnapshotFirecloudGroupStepTest {

  @Mock private IamService iamService;
  @Mock private FlightContext flightContext;
  @Mock private FlightMap workingMap;
  private final UUID snapshotId = UUID.randomUUID();
  private final String snapshotIdString = String.valueOf(snapshotId);
  private CreateSnapshotFirecloudGroupStep step;

  @BeforeEach
  void setUp() {
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    step = new CreateSnapshotFirecloudGroupStep(iamService, snapshotId);
  }

  @Test
  void doStep() throws InterruptedException {
    String groupName = DuosService.constructFirecloudGroupName(snapshotIdString);
    when(iamService.createGroup(groupName)).thenReturn(snapshotIdString);
    assertEquals(StepResult.getStepResultSuccess(), step.doStep(flightContext));
    verify(workingMap).put(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP, groupName);
  }

  @Test
  void doStepTryAgain() throws InterruptedException {
    String oldGroupName = DuosService.constructFirecloudGroupName(snapshotIdString);
    when(iamService.createGroup(oldGroupName)).thenThrow(new IamConflictException(new Throwable()));
    when(iamService.createGroup(startsWith(oldGroupName))).thenReturn(snapshotIdString);
    assertEquals(StepResult.getStepResultSuccess(), step.doStep(flightContext));
    verify(workingMap)
        .put(eq(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP), startsWith(oldGroupName));
  }

  @Test
  void undoStepGroupCreated() throws InterruptedException {
    when(workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP, String.class))
        .thenReturn(snapshotIdString);
    assertEquals(StepResult.getStepResultSuccess(), step.undoStep(flightContext));
    verify(iamService).deleteGroup(snapshotIdString);
  }

  @Test
  void undoStepGroupNotCreated() throws InterruptedException {
    when(workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP, String.class))
        .thenReturn(null);
    assertEquals(StepResult.getStepResultSuccess(), step.undoStep(flightContext));
    verifyNoInteractions(iamService);
  }
}
