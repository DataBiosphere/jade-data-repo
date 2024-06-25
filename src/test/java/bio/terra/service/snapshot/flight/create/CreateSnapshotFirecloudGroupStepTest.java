package bio.terra.service.snapshot.flight.create;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamConflictException;
import bio.terra.service.auth.iam.exception.IamNotFoundException;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@Tag(Unit.TAG)
@ExtendWith(MockitoExtension.class)
class CreateSnapshotFirecloudGroupStepTest {
  @Mock private IamService iamService;
  @Mock private FlightContext flightContext;

  private static final String GROUP_NAME = "groupName";
  private static final String GROUP_EMAIL = "groupEmail";

  private CreateSnapshotFirecloudGroupStep step;

  @BeforeEach
  void setUp() {
    step = new CreateSnapshotFirecloudGroupStep(iamService);
  }

  @Test
  void doStep() throws InterruptedException {
    FlightMap workingMap = new FlightMap();
    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_NAME, GROUP_NAME);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    when(iamService.createGroup(GROUP_NAME)).thenReturn(GROUP_EMAIL);
    assertEquals(step.doStep(flightContext), StepResult.getStepResultSuccess());
    assertEquals(
        flightContext
            .getWorkingMap()
            .get(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_EMAIL, String.class),
        GROUP_EMAIL);
  }

  @Test
  void doStepIamConflict() throws InterruptedException {
    FlightMap workingMap = new FlightMap();
    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_NAME, GROUP_NAME);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    when(iamService.createGroup(GROUP_NAME)).thenThrow(IamConflictException.class);
    when(iamService.getGroup(GROUP_NAME)).thenReturn(GROUP_EMAIL);
    assertEquals(step.doStep(flightContext).getStepStatus(), StepStatus.STEP_RESULT_SUCCESS);
    assertEquals(
        flightContext
            .getWorkingMap()
            .get(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_EMAIL, String.class),
        GROUP_EMAIL);
  }

  @Test
  void doStepIamNotFound() throws InterruptedException {
    FlightMap workingMap = new FlightMap();
    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_NAME, GROUP_NAME);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    when(iamService.createGroup(GROUP_NAME)).thenThrow(IamConflictException.class);
    when(iamService.getGroup(GROUP_NAME)).thenThrow(IamNotFoundException.class);
    assertEquals(step.doStep(flightContext).getStepStatus(), StepStatus.STEP_RESULT_RESTART_FLIGHT);
    assertNull(
        flightContext
            .getWorkingMap()
            .get(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_EMAIL, String.class));
  }

  @Test
  void undoStep() throws InterruptedException {
    FlightMap workingMap = new FlightMap();
    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_NAME, GROUP_NAME);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    assertEquals(step.undoStep(flightContext), StepResult.getStepResultSuccess());
    verify(iamService).deleteGroup(GROUP_NAME);
  }

  @Test
  void undoStepIamNotFound() throws InterruptedException {
    FlightMap workingMap = new FlightMap();
    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_NAME, GROUP_NAME);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    doThrow(IamNotFoundException.class).when(iamService).deleteGroup(GROUP_NAME);
    assertEquals(step.undoStep(flightContext), StepResult.getStepResultSuccess());
  }
}
