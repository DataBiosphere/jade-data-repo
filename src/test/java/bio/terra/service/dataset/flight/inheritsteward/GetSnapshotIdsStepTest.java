package bio.terra.service.dataset.flight.inheritsteward;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.Arrays;
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
class GetSnapshotIdsStepTest {

  @Mock private SnapshotDao snapshotDao;
  @Mock private FlightContext flightContext;
  private static final UUID DATASET_ID = UUID.randomUUID();
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private GetSnapshotIdsStep step;

  @BeforeEach
  void setUp() {
    step = new GetSnapshotIdsStep(snapshotDao, DATASET_ID, TEST_USER);
  }

  @Test
  void doStep() throws Exception {
    List<UUID> snapshotIds = Arrays.asList(UUID.randomUUID(), UUID.randomUUID());
    FlightMap workingMap = new FlightMap();
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    when(snapshotDao.getSnapshotIds(DATASET_ID)).thenReturn(snapshotIds);
    assertEquals(step.doStep(flightContext), StepResult.getStepResultSuccess());
    assertEquals(workingMap.get(DatasetWorkingMapKeys.SNAPSHOT_IDS, List.class), snapshotIds);
  }
}
