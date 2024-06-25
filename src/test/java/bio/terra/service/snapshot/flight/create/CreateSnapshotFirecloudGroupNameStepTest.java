package bio.terra.service.snapshot.flight.create;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.startsWith;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamNotFoundException;
import bio.terra.service.duos.DuosService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.Objects;
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
    String groupName = DuosService.constructFirecloudGroupName(String.valueOf(snapshotId));
    when(iamService.getGroup(startsWith(groupName)))
        .thenThrow(new IamNotFoundException(new Throwable("Group not found")));
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    assertEquals(StepResult.getStepResultSuccess(), step.doStep(flightContext));
    assertTrue(
        Objects.requireNonNull(
                workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_FIRECLOUD_GROUP_NAME, String.class))
            .startsWith(groupName));
  }

  @Test
  void doStepFailure() {
    assertThrows(InterruptedException.class, () -> step.doStep(flightContext));
    verifyNoInteractions(flightContext);
  }

  @Test
  void undoStep() throws InterruptedException {
    assertEquals(step.undoStep(flightContext), StepResult.getStepResultSuccess());
  }
}
