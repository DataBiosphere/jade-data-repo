package bio.terra.service.journal.flight;

import static bio.terra.service.common.CommonFlightUtils.getFlightInformationOfInterest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.common.JournalRecordDeleteEntryStep;
import bio.terra.service.journal.JournalService;
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
public class JournalRecordDeleteEntryStepTest {
  @Mock private JournalService journalService;
  @Mock private FlightContext flightContext;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  private static final UUID JOURNAL_ENTRY_KEY = UUID.randomUUID();
  private static final UUID DATASET_ID = UUID.randomUUID();
  private JournalRecordDeleteEntryStep step;

  @Before
  public void setup() {
    step =
        new JournalRecordDeleteEntryStep(
            journalService, TEST_USER, DATASET_ID, IamResourceType.DATASET, "foo");
    FlightMap workingMap = new FlightMap();
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
  }

  @Test
  public void testDoAndUndoStep() throws InterruptedException {
    when(journalService.recordDelete(
            TEST_USER,
            DATASET_ID,
            IamResourceType.DATASET,
            "foo",
            getFlightInformationOfInterest(flightContext)))
        .thenReturn(JOURNAL_ENTRY_KEY);
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(journalService)
        .recordDelete(
            TEST_USER,
            DATASET_ID,
            IamResourceType.DATASET,
            "foo",
            getFlightInformationOfInterest(flightContext));
    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(journalService).removeJournalEntry(JOURNAL_ENTRY_KEY);
  }

  @Test
  public void testNullJournalEntryKey() throws InterruptedException {
    when(journalService.recordDelete(
            TEST_USER,
            DATASET_ID,
            IamResourceType.DATASET,
            "foo",
            getFlightInformationOfInterest(flightContext)))
        .thenReturn(null);
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(journalService)
        .recordDelete(
            TEST_USER,
            DATASET_ID,
            IamResourceType.DATASET,
            "foo",
            getFlightInformationOfInterest(flightContext));
    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(journalService, never()).removeJournalEntry(JOURNAL_ENTRY_KEY);
  }
}
