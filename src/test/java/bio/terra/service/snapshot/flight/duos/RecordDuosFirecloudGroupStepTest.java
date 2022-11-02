package bio.terra.service.snapshot.flight.duos;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.DuosFixtures;
import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.service.duos.DuosDao;
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
public class RecordDuosFirecloudGroupStepTest {

  @MockBean private DuosDao duosDao;
  @Mock private FlightContext flightContext;

  private static final String DUOS_ID = "DUOS-123456";
  private static final UUID DUOS_FIRECLOUD_GROUP_ID = UUID.randomUUID();

  private RecordDuosFirecloudGroupStep step;
  private DuosFirecloudGroupModel duosFirecloudGroupCreated;
  private DuosFirecloudGroupModel duosFirecloudGroupRetrieved;
  private FlightMap workingMap;

  @Before
  public void setup() {
    step = new RecordDuosFirecloudGroupStep(duosDao);

    duosFirecloudGroupCreated = DuosFixtures.duosFirecloudGroupCreated(DUOS_ID);
    duosFirecloudGroupRetrieved =
        DuosFixtures.duosFirecloudGroupFromDb(DUOS_ID, DUOS_FIRECLOUD_GROUP_ID);

    workingMap = new FlightMap();
    workingMap.put(SnapshotDuosMapKeys.FIRECLOUD_GROUP, duosFirecloudGroupCreated);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
  }

  @Test
  public void testDoAndUndoStepSucceeds() throws InterruptedException {
    when(duosDao.insertFirecloudGroup(duosFirecloudGroupCreated))
        .thenReturn(DUOS_FIRECLOUD_GROUP_ID);
    when(duosDao.retrieveFirecloudGroup(DUOS_FIRECLOUD_GROUP_ID))
        .thenReturn(duosFirecloudGroupRetrieved);

    StepResult doResult = step.doStep(flightContext);
    assertEquals(doResult.getStepStatus(), StepStatus.STEP_RESULT_SUCCESS);
    assertEquals(
        "Retrieved Firecloud group overwrites created in working map",
        SnapshotDuosFlightUtils.getFirecloudGroup(flightContext),
        duosFirecloudGroupRetrieved);

    // Undoing when we recorded the group deletes the record
    StepResult undoResult = step.undoStep(flightContext);
    assertEquals(undoResult.getStepStatus(), StepStatus.STEP_RESULT_SUCCESS);
    verify(duosDao, times(1)).deleteFirecloudGroup(DUOS_FIRECLOUD_GROUP_ID);
  }

  @Test
  public void testDoStepThrows() throws InterruptedException {
    doThrow(RuntimeException.class).when(duosDao).insertFirecloudGroup(duosFirecloudGroupCreated);
    assertThrows(RuntimeException.class, () -> step.doStep(flightContext));

    assertEquals(
        "Created Firecloud group remains in working map when insertion fails",
        SnapshotDuosFlightUtils.getFirecloudGroup(flightContext),
        duosFirecloudGroupCreated);

    // Undoing when we failed to record the group is a no-op
    StepResult undoResult = step.undoStep(flightContext);
    assertEquals(undoResult.getStepStatus(), StepStatus.STEP_RESULT_SUCCESS);
    verify(duosDao, never()).deleteFirecloudGroup(any());
  }
}
