package bio.terra.service.snapshot.flight.duos;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.DuosFixtures;
import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.model.SnapshotLinkDuosDatasetResponse;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.exception.SnapshotUpdateException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.test.context.ActiveProfiles;

@RunWith(MockitoJUnitRunner.StrictStubs.class)
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class UpdateSnapshotDuosFirecloudGroupIdStepTest {

  @Mock private SnapshotDao snapshotDao;
  @Mock private FlightContext flightContext;

  private static final DuosFirecloudGroupModel DUOS_FIRECLOUD_GROUP =
      DuosFixtures.createDbFirecloudGroup("DUOS-123456");
  private static final DuosFirecloudGroupModel DUOS_FIRECLOUD_GROUP_PREV =
      DuosFixtures.createDbFirecloudGroup("DUOS-789012");
  private static final UUID SNAPSHOT_ID = UUID.randomUUID();

  private UpdateSnapshotDuosFirecloudGroupIdStep step;
  private FlightMap workingMap;

  @Before
  public void setup() {
    workingMap = new FlightMap();
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
  }

  @Test
  public void testDoAndUndoStepUnlink() throws InterruptedException {
    step =
        new UpdateSnapshotDuosFirecloudGroupIdStep(
            snapshotDao, SNAPSHOT_ID, DUOS_FIRECLOUD_GROUP_PREV);

    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(snapshotDao).updateDuosFirecloudGroupId(SNAPSHOT_ID, null);

    SnapshotLinkDuosDatasetResponse response =
        workingMap.get(JobMapKeys.RESPONSE.getKeyName(), SnapshotLinkDuosDatasetResponse.class);
    assertNotNull(response);
    assertNull("No new DUOS dataset to link", response.getLinked());
    assertThat(
        "Unlinked our previous DUOS dataset",
        response.getUnlinked(),
        equalTo(DUOS_FIRECLOUD_GROUP_PREV));

    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(snapshotDao).updateDuosFirecloudGroupId(SNAPSHOT_ID, DUOS_FIRECLOUD_GROUP_PREV.getId());
  }

  @Test
  public void testDoAndUndoStepLink() throws InterruptedException {
    step = new UpdateSnapshotDuosFirecloudGroupIdStep(snapshotDao, SNAPSHOT_ID, null);
    workingMap.put(SnapshotDuosMapKeys.FIRECLOUD_GROUP, DUOS_FIRECLOUD_GROUP);

    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(snapshotDao).updateDuosFirecloudGroupId(SNAPSHOT_ID, DUOS_FIRECLOUD_GROUP.getId());

    SnapshotLinkDuosDatasetResponse response =
        workingMap.get(JobMapKeys.RESPONSE.getKeyName(), SnapshotLinkDuosDatasetResponse.class);
    assertNotNull(response);
    assertThat("Linked a new DUOS dataset", response.getLinked(), equalTo(DUOS_FIRECLOUD_GROUP));
    assertNull("No previous DUOS dataset to unlink", response.getUnlinked());

    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(snapshotDao).updateDuosFirecloudGroupId(SNAPSHOT_ID, null);
  }

  @Test
  public void testDoAndUndoStepUpdateLink() throws InterruptedException {
    step =
        new UpdateSnapshotDuosFirecloudGroupIdStep(
            snapshotDao, SNAPSHOT_ID, DUOS_FIRECLOUD_GROUP_PREV);
    workingMap.put(SnapshotDuosMapKeys.FIRECLOUD_GROUP, DUOS_FIRECLOUD_GROUP);

    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(snapshotDao).updateDuosFirecloudGroupId(SNAPSHOT_ID, DUOS_FIRECLOUD_GROUP.getId());

    SnapshotLinkDuosDatasetResponse response =
        workingMap.get(JobMapKeys.RESPONSE.getKeyName(), SnapshotLinkDuosDatasetResponse.class);
    assertNotNull(response);
    assertThat("Linked a new DUOS dataset", response.getLinked(), equalTo(DUOS_FIRECLOUD_GROUP));
    assertThat(
        "Unlinked our previous DUOS dataset",
        response.getUnlinked(),
        equalTo(DUOS_FIRECLOUD_GROUP_PREV));

    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(snapshotDao).updateDuosFirecloudGroupId(SNAPSHOT_ID, DUOS_FIRECLOUD_GROUP_PREV.getId());
  }

  @Test
  public void testDoAndUndoUnlinkNoOp() throws InterruptedException {
    // The caller could conceivably trigger a flight to unlink a snapshot from its DUOS dataset,
    // despite the snapshot having no existing link.  In the user's eyes this is a no-op.
    step = new UpdateSnapshotDuosFirecloudGroupIdStep(snapshotDao, SNAPSHOT_ID, null);

    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(snapshotDao).updateDuosFirecloudGroupId(SNAPSHOT_ID, null);

    SnapshotLinkDuosDatasetResponse response =
        workingMap.get(JobMapKeys.RESPONSE.getKeyName(), SnapshotLinkDuosDatasetResponse.class);
    assertNotNull(response);
    assertNull("No new DUOS dataset to link", response.getLinked());
    assertNull("No previous DUOS dataset to unlink", response.getUnlinked());

    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(snapshotDao, times(2)).updateDuosFirecloudGroupId(SNAPSHOT_ID, null);
  }

  @Test
  public void testDoAndUndoStepFails() throws InterruptedException {
    step =
        new UpdateSnapshotDuosFirecloudGroupIdStep(
            snapshotDao, SNAPSHOT_ID, DUOS_FIRECLOUD_GROUP_PREV);
    workingMap.put(SnapshotDuosMapKeys.FIRECLOUD_GROUP, DUOS_FIRECLOUD_GROUP);
    doThrow(SnapshotUpdateException.class)
        .when(snapshotDao)
        .updateDuosFirecloudGroupId(SNAPSHOT_ID, DUOS_FIRECLOUD_GROUP.getId());

    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_FATAL));
    verify(snapshotDao).updateDuosFirecloudGroupId(SNAPSHOT_ID, DUOS_FIRECLOUD_GROUP.getId());

    assertTrue(doResult.getException().isPresent());
    assertThat(doResult.getException().get(), instanceOf(SnapshotUpdateException.class));

    SnapshotLinkDuosDatasetResponse response =
        workingMap.get(JobMapKeys.RESPONSE.getKeyName(), SnapshotLinkDuosDatasetResponse.class);
    assertNull(response);

    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(snapshotDao).updateDuosFirecloudGroupId(SNAPSHOT_ID, DUOS_FIRECLOUD_GROUP_PREV.getId());
  }
}