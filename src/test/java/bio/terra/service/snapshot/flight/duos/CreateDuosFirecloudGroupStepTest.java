package bio.terra.service.snapshot.flight.duos;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertThrows;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.verifyNoInteractions;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.DuosFixtures;
import bio.terra.model.DuosFirecloudGroupModel;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.duos.DuosService;
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
public class CreateDuosFirecloudGroupStepTest {

  @Mock private DuosService duosService;
  @Mock private IamService iamService;
  @Mock private FlightContext flightContext;

  private static final String DUOS_ID = "DUOS-123456";
  private static final DuosFirecloudGroupModel CREATED = DuosFixtures.createFirecloudGroup(DUOS_ID);

  private CreateDuosFirecloudGroupStep step;
  private FlightMap workingMap;

  @Before
  public void setup() {
    step = new CreateDuosFirecloudGroupStep(duosService, iamService, DUOS_ID);

    workingMap = new FlightMap();
    workingMap.put(SnapshotDuosMapKeys.FIRECLOUD_GROUP_RETRIEVED, false);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
  }

  @Test
  public void testDoAndUndoStepSucceeds() throws InterruptedException {
    when(duosService.createFirecloudGroup(DUOS_ID)).thenReturn(CREATED);

    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(duosService).createFirecloudGroup(DUOS_ID);

    assertThat(
        "Created Firecloud group is stored in working map",
        SnapshotDuosFlightUtils.getFirecloudGroup(flightContext),
        equalTo(CREATED));
    assertFalse(
        "Working map's earlier record of Firecloud group retrieval is unchanged",
        workingMap.get(SnapshotDuosMapKeys.FIRECLOUD_GROUP_RETRIEVED, boolean.class));

    // Undoing when we created a group deletes the group
    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(iamService).deleteGroup(CREATED.getFirecloudGroupName());
  }

  @Test
  public void testDoAndUndoStepThrows() throws InterruptedException {
    doThrow(RuntimeException.class).when(duosService).createFirecloudGroup(DUOS_ID);

    assertThrows(RuntimeException.class, () -> step.doStep(flightContext));
    verify(duosService).createFirecloudGroup(DUOS_ID);

    assertNull(
        "No Firecloud group is added to the working map when creation fails",
        SnapshotDuosFlightUtils.getFirecloudGroup(flightContext));
    assertFalse(
        "Working map's earlier record of Firecloud group retrieval is unchanged",
        workingMap.get(SnapshotDuosMapKeys.FIRECLOUD_GROUP_RETRIEVED, boolean.class));

    // Undoing when we failed to create a group is a no-op
    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verifyNoInteractions(iamService);
  }
}
