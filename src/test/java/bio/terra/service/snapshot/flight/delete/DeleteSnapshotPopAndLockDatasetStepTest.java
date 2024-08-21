package bio.terra.service.snapshot.flight.delete;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
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
class DeleteSnapshotPopAndLockDatasetStepTest {

  @Mock SnapshotService snapshotService;
  @Mock DatasetService datasetService;
  private Snapshot snapshot;
  private UUID snapshotId;
  private UUID datasetId;
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private final boolean sharedLock = true;
  @Mock private FlightContext flightContext;
  private DeleteSnapshotPopAndLockDatasetStep step;

  @BeforeEach
  void setUp() {
    snapshotId = UUID.randomUUID();
    datasetId = UUID.randomUUID();
    snapshot =
        new Snapshot()
            .id(snapshotId)
            .snapshotSources(List.of(new SnapshotSource().dataset(new Dataset().id(datasetId))));
    step =
        new DeleteSnapshotPopAndLockDatasetStep(
            snapshotId, snapshotService, datasetService, TEST_USER, sharedLock);
  }

  // snapshot and dataset exist
  @Test
  void doStep() {
    when(snapshotService.retrieve(snapshotId)).thenReturn(snapshot);
    var flightMap = new FlightMap();
    when(flightContext.getWorkingMap()).thenReturn(flightMap);
    assertThat(
        "Step is successful",
        step.doStep(flightContext),
        equalTo(StepResult.getStepResultSuccess()));

    FlightMap map = flightContext.getWorkingMap();
    assertEquals(true, map.get(SnapshotWorkingMapKeys.SNAPSHOT_EXISTS, Boolean.class));
    assertEquals(false, map.get(SnapshotWorkingMapKeys.SNAPSHOT_HAS_GOOGLE_PROJECT, Boolean.class));
    assertEquals(
        false, map.get(SnapshotWorkingMapKeys.SNAPSHOT_HAS_AZURE_STORAGE_ACCOUNT, Boolean.class));
    assertEquals(datasetId, map.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class));
    verify(datasetService).lock(eq(datasetId), any(), eq(sharedLock));
    assertEquals(true, map.get(SnapshotWorkingMapKeys.DATASET_EXISTS, Boolean.class));
  }

  // snapshot does not exist
  @Test
  void doStepSnapshotNotFound() {
    when(snapshotService.retrieve(snapshotId))
        .thenThrow(new SnapshotNotFoundException("not found"));
    var flightMap = new FlightMap();
    when(flightContext.getWorkingMap()).thenReturn(flightMap);
    assertThat(
        "Step is successful",
        step.doStep(flightContext),
        equalTo(StepResult.getStepResultSuccess()));

    FlightMap map = flightContext.getWorkingMap();
    assertEquals(false, map.get(SnapshotWorkingMapKeys.SNAPSHOT_EXISTS, Boolean.class));
    assertEquals(false, map.get(SnapshotWorkingMapKeys.DATASET_EXISTS, Boolean.class));

    assertEquals(false, map.get(SnapshotWorkingMapKeys.SNAPSHOT_HAS_GOOGLE_PROJECT, Boolean.class));
    assertEquals(
        false, map.get(SnapshotWorkingMapKeys.SNAPSHOT_HAS_AZURE_STORAGE_ACCOUNT, Boolean.class));
  }

  // dataset does not exist
  @Test
  void doStepDatasetNotFound() {
    when(snapshotService.retrieve(snapshotId)).thenReturn(snapshot);
    var flightMap = new FlightMap();
    when(flightContext.getWorkingMap()).thenReturn(flightMap);
    doThrow(new DatasetNotFoundException("not found"))
        .when(datasetService)
        .lock(eq(datasetId), any(), eq(sharedLock));
    assertThat(
        "Step is successful",
        step.doStep(flightContext),
        equalTo(StepResult.getStepResultSuccess()));

    FlightMap map = flightContext.getWorkingMap();
    assertEquals(true, map.get(SnapshotWorkingMapKeys.SNAPSHOT_EXISTS, Boolean.class));
    assertEquals(false, map.get(SnapshotWorkingMapKeys.DATASET_EXISTS, Boolean.class));
  }
}
