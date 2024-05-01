package bio.terra.service.snapshot.flight.duos;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.DuosFixtures;
import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.service.duos.DuosService;
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

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SyncDuosFirecloudGroupStepTest {

  @Mock private DuosService duosService;
  @Mock private FlightContext flightContext;

  private static final String DUOS_ID = "DUOS-123456";

  private static final DuosFirecloudGroupModel INSERTED =
      DuosFixtures.createDbFirecloudGroup(DUOS_ID);
  private static final DuosFirecloudGroupModel SYNCED =
      DuosFixtures.createDbSyncedFirecloudGroup(DUOS_ID);

  private SyncDuosFirecloudGroupStep step;

  @BeforeEach
  void setup() {
    step = new SyncDuosFirecloudGroupStep(duosService, DUOS_ID);

    FlightMap workingMap = new FlightMap();
    workingMap.put(SnapshotDuosMapKeys.FIRECLOUD_GROUP, INSERTED);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
  }

  @Test
  void testDoStepSucceeds() throws InterruptedException {
    when(duosService.syncDuosDatasetAuthorizedUsers(DUOS_ID)).thenReturn(SYNCED);

    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(duosService).syncDuosDatasetAuthorizedUsers(DUOS_ID);

    assertThat(
        "Synced Firecloud group overwrites inserted in working map",
        SnapshotDuosFlightUtils.getFirecloudGroup(flightContext),
        equalTo(SYNCED));
  }

  @Test
  void testDoStepThrows() {
    doThrow(RuntimeException.class).when(duosService).syncDuosDatasetAuthorizedUsers(DUOS_ID);
    assertThrows(RuntimeException.class, () -> step.doStep(flightContext));

    assertThat(
        "Inserted Firecloud group remains in working map when sync fails",
        SnapshotDuosFlightUtils.getFirecloudGroup(flightContext),
        equalTo(INSERTED));
  }
}
