package bio.terra.service.journal.flight;

import static bio.terra.service.common.CommonFlightUtils.getFlightInformationOfInterest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.journal.JournalService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.snapshot.flight.create.CreateSnapshotJournalEntryStep;
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
public class CreateSnapshotJournalEntryStepTest {
  @Mock private JournalService journalService;
  @Mock private FlightContext flightContext;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  private static final String FLIGHT_ID = UUID.randomUUID().toString();
  private static final UUID SNAPSHOT_ID = UUID.randomUUID();
  private CreateSnapshotJournalEntryStep step;

  @Before
  public void setup() {
    step =
        new CreateSnapshotJournalEntryStep(
            journalService, TEST_USER, IamResourceType.DATASNAPSHOT, "foo", false);
    FlightMap workingMap = new FlightMap();
    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_ID, SNAPSHOT_ID);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    when(flightContext.getFlightId()).thenReturn(FLIGHT_ID);
  }

  @Test
  public void testDoAndUndoStep() throws InterruptedException {
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(journalService)
        .recordCreate(
            TEST_USER,
            SNAPSHOT_ID,
            IamResourceType.DATASNAPSHOT,
            "foo",
            getFlightInformationOfInterest(flightContext),
            false);
    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(journalService).removeJournalEntriesByFlightId(FLIGHT_ID);
  }
}
