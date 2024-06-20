package bio.terra.service.snapshot.flight.create;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.auth.iam.FirecloudGroupModel;
import bio.terra.service.auth.iam.IamService;
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
    String groupName = snapshotIdString + "-users";
    String groupEmail = groupName + "@firecloud.org";
    when(iamService.createFirecloudGroup(snapshotIdString))
        .thenReturn(new FirecloudGroupModel().groupName(groupName).groupEmail(groupEmail));
    assertEquals(StepResult.getStepResultSuccess(), step.doStep(flightContext));
    verify(workingMap).put(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP, groupName);
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
