package bio.terra.service.snapshot.flight.duos;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
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
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class UpdateSnapshotDuosFirecloudGroupIdStepTest {

  @MockBean private SnapshotDao snapshotDao;
  @Mock private FlightContext flightContext;

  private static final String DUOS_ID = "DUOS-123456";
  private static final String DUOS_ID_PREV = "DUOS-789012";
  private static final UUID SNAPSHOT_ID = UUID.randomUUID();

  private DuosFirecloudGroupModel duosFirecloudGroup;
  private DuosFirecloudGroupModel duosFirecloudGroupPrev;
  private UpdateSnapshotDuosFirecloudGroupIdStep step;
  private FlightMap workingMap;

  @Before
  public void setup() {
    duosFirecloudGroup = DuosFixtures.duosFirecloudGroupFromDb(DUOS_ID);
    duosFirecloudGroupPrev = DuosFixtures.duosFirecloudGroupFromDb(DUOS_ID_PREV);

    workingMap = new FlightMap();
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
  }

  @Test
  public void testDoAndUndoStepUnlink() throws InterruptedException {
    step =
        new UpdateSnapshotDuosFirecloudGroupIdStep(
            snapshotDao, SNAPSHOT_ID, duosFirecloudGroupPrev);

    StepResult doResult = step.doStep(flightContext);
    assertEquals(doResult.getStepStatus(), StepStatus.STEP_RESULT_SUCCESS);
    verify(snapshotDao, times(1)).updateDuosFirecloudGroupId(SNAPSHOT_ID, null);

    SnapshotLinkDuosDatasetResponse response =
        workingMap.get(JobMapKeys.RESPONSE.getKeyName(), SnapshotLinkDuosDatasetResponse.class);
    assertNotNull(response);
    assertNull("No new DUOS dataset to link", response.getLinked());
    assertEquals(
        "Unlinked our previous DUOS dataset", response.getUnlinked(), duosFirecloudGroupPrev);

    StepResult undoResult = step.undoStep(flightContext);
    assertEquals(undoResult.getStepStatus(), StepStatus.STEP_RESULT_SUCCESS);
    verify(snapshotDao, times(1))
        .updateDuosFirecloudGroupId(SNAPSHOT_ID, duosFirecloudGroupPrev.getId());
  }

  @Test
  public void testDoAndUndoStepLink() throws InterruptedException {
    step = new UpdateSnapshotDuosFirecloudGroupIdStep(snapshotDao, SNAPSHOT_ID, null);
    workingMap.put(SnapshotDuosMapKeys.FIRECLOUD_GROUP, duosFirecloudGroup);

    StepResult doResult = step.doStep(flightContext);
    assertEquals(doResult.getStepStatus(), StepStatus.STEP_RESULT_SUCCESS);
    verify(snapshotDao, times(1))
        .updateDuosFirecloudGroupId(SNAPSHOT_ID, duosFirecloudGroup.getId());

    SnapshotLinkDuosDatasetResponse response =
        workingMap.get(JobMapKeys.RESPONSE.getKeyName(), SnapshotLinkDuosDatasetResponse.class);
    assertNotNull(response);
    assertEquals("Linked a new DUOS dataset", response.getLinked(), duosFirecloudGroup);
    assertNull("No previous DUOS dataset to unlink", response.getUnlinked());

    StepResult undoResult = step.undoStep(flightContext);
    assertEquals(undoResult.getStepStatus(), StepStatus.STEP_RESULT_SUCCESS);
    verify(snapshotDao, times(1)).updateDuosFirecloudGroupId(SNAPSHOT_ID, null);
  }

  @Test
  public void testDoAndUndoStepUpdateLink() throws InterruptedException {
    step =
        new UpdateSnapshotDuosFirecloudGroupIdStep(
            snapshotDao, SNAPSHOT_ID, duosFirecloudGroupPrev);
    workingMap.put(SnapshotDuosMapKeys.FIRECLOUD_GROUP, duosFirecloudGroup);

    StepResult doResult = step.doStep(flightContext);
    assertEquals(doResult.getStepStatus(), StepStatus.STEP_RESULT_SUCCESS);
    verify(snapshotDao, times(1))
        .updateDuosFirecloudGroupId(SNAPSHOT_ID, duosFirecloudGroup.getId());

    SnapshotLinkDuosDatasetResponse response =
        workingMap.get(JobMapKeys.RESPONSE.getKeyName(), SnapshotLinkDuosDatasetResponse.class);
    assertNotNull(response);
    assertEquals("Linked a new DUOS dataset", response.getLinked(), duosFirecloudGroup);
    assertEquals(
        "Unlinked our previous DUOS dataset", response.getUnlinked(), duosFirecloudGroupPrev);

    StepResult undoResult = step.undoStep(flightContext);
    assertEquals(undoResult.getStepStatus(), StepStatus.STEP_RESULT_SUCCESS);
    verify(snapshotDao, times(1))
        .updateDuosFirecloudGroupId(SNAPSHOT_ID, duosFirecloudGroupPrev.getId());
  }

  @Test
  public void testDoAndUndoStepFails() throws InterruptedException {
    step =
        new UpdateSnapshotDuosFirecloudGroupIdStep(
            snapshotDao, SNAPSHOT_ID, duosFirecloudGroupPrev);
    workingMap.put(SnapshotDuosMapKeys.FIRECLOUD_GROUP, duosFirecloudGroup);
    doThrow(SnapshotUpdateException.class)
        .when(snapshotDao)
        .updateDuosFirecloudGroupId(SNAPSHOT_ID, duosFirecloudGroup.getId());

    StepResult doResult = step.doStep(flightContext);
    assertEquals(doResult.getStepStatus(), StepStatus.STEP_RESULT_FAILURE_FATAL);
    verify(snapshotDao, times(1))
        .updateDuosFirecloudGroupId(SNAPSHOT_ID, duosFirecloudGroup.getId());

    assertTrue(doResult.getException().isPresent());
    assertThat(doResult.getException().get(), instanceOf(SnapshotUpdateException.class));

    SnapshotLinkDuosDatasetResponse response =
        workingMap.get(JobMapKeys.RESPONSE.getKeyName(), SnapshotLinkDuosDatasetResponse.class);
    assertNull(response);

    StepResult undoResult = step.undoStep(flightContext);
    assertEquals(undoResult.getStepStatus(), StepStatus.STEP_RESULT_SUCCESS);
    verify(snapshotDao, times(1))
        .updateDuosFirecloudGroupId(SNAPSHOT_ID, duosFirecloudGroupPrev.getId());
  }
}
