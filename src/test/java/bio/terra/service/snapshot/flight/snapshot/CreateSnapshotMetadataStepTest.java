package bio.terra.service.snapshot.flight.snapshot;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.fixtures.DuosFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.snapshot.flight.create.CreateSnapshotMetadataStep;
import bio.terra.service.snapshot.flight.duos.SnapshotDuosMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.context.ActiveProfiles;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class CreateSnapshotMetadataStepTest {

  @Mock private SnapshotDao snapshotDao;
  @Mock private SnapshotService snapshotService;
  @Mock private FlightContext flightContext;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();
  private static final String FLIGHT_ID = String.valueOf(UUID.randomUUID());
  private static final UUID SNAPSHOT_ID = UUID.randomUUID();
  private static final UUID PROJECT_RESOURCE_ID = UUID.randomUUID();
  private static final String DUOS_ID = "DUOS-123456";
  private static final DuosFirecloudGroupModel DUOS_FIRECLOUD_GROUP =
      DuosFixtures.createDbFirecloudGroup(DUOS_ID);

  private CreateSnapshotMetadataStep step;
  private FlightMap workingMap;
  private SnapshotRequestModel snapshotRequestModel;
  private Snapshot snapshot;

  @Before
  public void setup() {
    workingMap = new FlightMap();
    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_ID, SNAPSHOT_ID);
    workingMap.put(SnapshotWorkingMapKeys.PROJECT_RESOURCE_ID, PROJECT_RESOURCE_ID);
    when(flightContext.getFlightId()).thenReturn(FLIGHT_ID);

    snapshotRequestModel = new SnapshotRequestModel();
    snapshot = new Snapshot();
    when(snapshotService.makeSnapshotFromSnapshotRequest(snapshotRequestModel))
        .thenReturn(snapshot);
  }

  @Test
  public void testDoAndUndoStep() throws InterruptedException {
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    step =
        new CreateSnapshotMetadataStep(
            snapshotDao, snapshotService, snapshotRequestModel, TEST_USER);
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    snapshot.id(UUID.randomUUID()).projectResourceId(PROJECT_RESOURCE_ID);
    ArgumentCaptor<Snapshot> argument = ArgumentCaptor.forClass(Snapshot.class);
    verify(snapshotDao).createAndLock(argument.capture(), eq(FLIGHT_ID), eq(TEST_USER));
    assertNull(argument.getValue().getDuosFirecloudGroupId());

    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(snapshotDao).delete(SNAPSHOT_ID, TEST_USER);
  }

  @Test
  public void testDoAndUndoStepWithDUOS() throws InterruptedException {
    workingMap.put(SnapshotDuosMapKeys.FIRECLOUD_GROUP, DUOS_FIRECLOUD_GROUP);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    snapshotRequestModel.duosId(DUOS_ID);

    step =
        new CreateSnapshotMetadataStep(
            snapshotDao, snapshotService, snapshotRequestModel, TEST_USER);
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    snapshot
        .id(SNAPSHOT_ID)
        .projectResourceId(PROJECT_RESOURCE_ID)
        .duosFirecloudGroupId(DUOS_FIRECLOUD_GROUP.getId());
    ArgumentCaptor<Snapshot> argument = ArgumentCaptor.forClass(Snapshot.class);
    verify(snapshotDao).createAndLock(argument.capture(), eq(FLIGHT_ID), eq(TEST_USER));
    assertEquals(argument.getValue().getDuosFirecloudGroupId(), DUOS_FIRECLOUD_GROUP.getId());

    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(snapshotDao).delete(SNAPSHOT_ID, TEST_USER);
  }
}
