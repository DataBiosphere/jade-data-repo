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
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.dataset.flight.create.CreateDatasetJournalEntryStep;
import bio.terra.service.journal.JournalService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class CreateDatasetJournalEntryStepTest {
  @Mock private JournalService journalService;
  @Mock private FlightContext flightContext;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  private static final String FLIGHT_ID = UUID.randomUUID().toString();
  private static final UUID DATASET_ID = UUID.randomUUID();
  private CreateDatasetJournalEntryStep step;

  @BeforeEach
  void setup() {
    step = new CreateDatasetJournalEntryStep(journalService, TEST_USER);
    FlightMap workingMap = new FlightMap();
    workingMap.put(DatasetWorkingMapKeys.DATASET_ID, DATASET_ID);
    when(flightContext.getWorkingMap()).thenReturn(workingMap);
    when(flightContext.getFlightId()).thenReturn(FLIGHT_ID);
  }

  @Test
  void testDoAndUndoStep() throws InterruptedException {
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(journalService)
        .recordCreate(
            TEST_USER,
            DATASET_ID,
            IamResourceType.DATASET,
            "Created dataset.",
            getFlightInformationOfInterest(flightContext),
            false);
    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(journalService).removeJournalEntriesByFlightId(FLIGHT_ID);
  }
}
