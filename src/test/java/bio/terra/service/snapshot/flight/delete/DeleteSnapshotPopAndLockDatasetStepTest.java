package bio.terra.service.snapshot.flight.delete;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepStatus;
import com.fasterxml.jackson.core.type.TypeReference;
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
class DeleteSnapshotPopAndLockDatasetStepTest {

  @Mock SnapshotService snapshotService;
  @Mock DatasetService datasetService;
  private Snapshot snapshot;
  private UUID snapshotId;
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private final boolean sharedLock = true;
  @Mock private FlightContext flightContext;
  private DeleteSnapshotPopAndLockDatasetStep step;

  @BeforeEach
  void setUp() {
    snapshotId = UUID.randomUUID();
    snapshot =
        new Snapshot()
            .id(snapshotId)
            .snapshotSources(
                List.of(new SnapshotSource().dataset(new Dataset().id(UUID.randomUUID()))));
    step =
        new DeleteSnapshotPopAndLockDatasetStep(
            snapshotId, snapshotService, datasetService, TEST_USER, sharedLock);
  }

  @Test
  void setAuthDomainGroups() {
    when(snapshotService.retrieve(snapshotId)).thenReturn(snapshot);
    when(snapshotService.retrieveAuthDomains(eq(snapshotId), any()))
        .thenReturn(List.of("group1", "group2"));
    var flightMap = new FlightMap();
    when(flightContext.getWorkingMap()).thenReturn(flightMap);
    step.doStep(flightContext);

    FlightMap map = flightContext.getWorkingMap();
    List<String> authDomains =
        map.get(SnapshotWorkingMapKeys.SNAPSHOT_AUTH_DOMAIN_GROUPS, new TypeReference<>() {});
    assertThat(
        "Auth domain list is populated", authDomains, containsInAnyOrder("group1", "group2"));
  }

  @Test
  void noAuthDomainGroups() {
    when(snapshotService.retrieve(snapshotId)).thenReturn(snapshot);
    when(snapshotService.retrieveAuthDomains(eq(snapshotId), any())).thenReturn(null);
    var flightMap = new FlightMap();
    when(flightContext.getWorkingMap()).thenReturn(flightMap);
    var result = step.doStep(flightContext);

    FlightMap map = flightContext.getWorkingMap();
    List<String> authDomains =
        map.get(SnapshotWorkingMapKeys.SNAPSHOT_AUTH_DOMAIN_GROUPS, new TypeReference<>() {});
    assertNull(authDomains);
    assertThat(result.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
  }
}
