package bio.terra.service.snapshot.flight.duos;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.never;
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
public class RecordDuosFirecloudGroupStepTest {

  @Mock private DuosDao duosDao;
  @Mock private FlightContext flightContext;

  private static final String DUOS_ID = "DUOS-123456";
  private static final DuosFirecloudGroupModel DUOS_FIRECLOUD_GROUP_CREATED =
      DuosFixtures.mockDuosFirecloudGroupCreated(DUOS_ID);
  private static final DuosFirecloudGroupModel DUOS_FIRECLOUD_GROUP_INSERTED =
      DuosFixtures.mockDuosFirecloudGroupFromDb(DUOS_ID);

  private RecordDuosFirecloudGroupStep step;
  private FlightMap workingMap;

  @Before
  public void setup() {
    step = new RecordDuosFirecloudGroupStep(duosDao);

    workingMap = new FlightMap();
    workingMap.put(SnapshotDuosMapKeys.FIRECLOUD_GROUP, DUOS_FIRECLOUD_GROUP_CREATED);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
  }

  @Test
  public void testDoAndUndoStepSucceeds() throws InterruptedException {
    when(duosDao.insertAndRetrieveFirecloudGroup(DUOS_FIRECLOUD_GROUP_CREATED))
        .thenReturn(DUOS_FIRECLOUD_GROUP_INSERTED);

    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    assertThat(
        "Inserted Firecloud group overwrites created in working map",
        SnapshotDuosFlightUtils.getFirecloudGroup(flightContext),
        equalTo(DUOS_FIRECLOUD_GROUP_INSERTED));

    // Undoing when we recorded the group deletes the record
    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(duosDao).deleteFirecloudGroup(DUOS_FIRECLOUD_GROUP_INSERTED.getId());
  }

  @Test
  public void testDoStepThrows() throws InterruptedException {
    doThrow(RuntimeException.class)
        .when(duosDao)
        .insertAndRetrieveFirecloudGroup(DUOS_FIRECLOUD_GROUP_CREATED);
    assertThrows(RuntimeException.class, () -> step.doStep(flightContext));

    assertThat(
        "Created Firecloud group remains in working map when insertion fails",
        SnapshotDuosFlightUtils.getFirecloudGroup(flightContext),
        equalTo(DUOS_FIRECLOUD_GROUP_CREATED));

    // Undoing when we failed to record the group is a no-op
    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(duosDao, never()).deleteFirecloudGroup(any());
  }
}
