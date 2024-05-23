package bio.terra.service.snapshot.flight.create;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class AddDataAccessControlsStepTest {
  @Mock private FlightContext flightContext;
  @Mock private SnapshotService snapshotService;

  @Mock private FlightMap workingMap;

  @Test
  void doStep() throws InterruptedException {
    AuthenticatedUserRequest TEST_USER = AuthenticationFixtures.randomUserRequest();
    List<String> dataAccessControls = List.of("group1", "group2");
    UUID snapshotId = UUID.randomUUID();

    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    when(workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class)).thenReturn(snapshotId);
    AddDataAccessControlsStep addDataAccessControlsStep =
        new AddDataAccessControlsStep(snapshotService, TEST_USER, dataAccessControls);
    StepResult result = addDataAccessControlsStep.doStep(flightContext);

    assertEquals(result, StepResult.getStepResultSuccess());
    verify(snapshotService)
        .addSnapshotDataAccessControls(TEST_USER, snapshotId, dataAccessControls);
  }
}
