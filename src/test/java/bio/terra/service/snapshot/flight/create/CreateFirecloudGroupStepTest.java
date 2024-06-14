package bio.terra.service.snapshot.flight.create;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class CreateFirecloudGroupStepTest {

  @Mock private IamService iamService;
  @Mock private FlightContext flightContext;
  @Mock private FlightMap workingMap;
  private final String snapshotName = "test";
  private final SnapshotRequestModel snapshotReq = new SnapshotRequestModel().name(snapshotName);
  private CreateFirecloudGroupStep step;

  @BeforeEach
  void setUp() {
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    step = new CreateFirecloudGroupStep(iamService, snapshotReq);
  }

  @Test
  void doStep() throws InterruptedException {
    when(iamService.createGroup(snapshotName)).thenReturn(snapshotName);
    assertEquals(StepResult.getStepResultSuccess(), step.doStep(flightContext));
    verify(workingMap).put(SnapshotWorkingMapKeys.SAM_GROUP, snapshotReq.getName());
  }

  @Test
  void undoStepGroupCreated() throws InterruptedException {
    when(workingMap.get(SnapshotWorkingMapKeys.SAM_GROUP, String.class)).thenReturn(snapshotName);
    assertEquals(StepResult.getStepResultSuccess(), step.undoStep(flightContext));
    verify(iamService).deleteGroup(snapshotName);
  }

  @Test
  void undoStepGroupNotCreated() throws InterruptedException {
    when(workingMap.get(SnapshotWorkingMapKeys.SAM_GROUP, String.class)).thenReturn(null);
    assertEquals(StepResult.getStepResultSuccess(), step.undoStep(flightContext));
    verifyNoInteractions(iamService);
  }
}
