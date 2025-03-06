package bio.terra.service.dataset.flight.inheritsteward;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class InheritStewardGetSnapshotIdsStepTest {

  @Mock private SnapshotService snapshotService;
  @Mock private FlightContext flightContext;
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private InheritStewardGetSnapshotIdsStep step;

  @BeforeEach
  void setUp() {
    step = new InheritStewardGetSnapshotIdsStep(snapshotService, DATASET_ID, TEST_USER);
  }

  @Test
  void doStep() throws Exception {
    List<UUID> snapshotIds = List.of(UUID.randomUUID(), UUID.randomUUID());
    FlightMap workingMap = new FlightMap();
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    when(snapshotService.enumerateSnapshotIdsForDataset(DATASET_ID, TEST_USER))
        .thenReturn(snapshotIds);
    assertEquals(step.doStep(flightContext), StepResult.getStepResultSuccess());
    assertEquals(workingMap.get(DatasetWorkingMapKeys.SNAPSHOT_IDS, List.class), snapshotIds);
  }
}
