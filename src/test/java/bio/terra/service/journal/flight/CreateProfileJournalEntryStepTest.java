package bio.terra.service.journal.flight;

import static bio.terra.service.common.CommonFlightUtils.getFlightInformationOfInterest;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.journal.JournalService;
import bio.terra.service.profile.flight.create.CreateProfileJournalEntryStep;
import bio.terra.stairway.FlightContext;
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
class CreateProfileJournalEntryStepTest {
  @Mock private JournalService journalService;
  @Mock private FlightContext flightContext;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  private static final String FLIGHT_ID = UUID.randomUUID().toString();
  private static final UUID BILLING_PROFILE_ID = UUID.randomUUID();
  private CreateProfileJournalEntryStep step;

  @BeforeEach
  void setup() {
    BillingProfileRequestModel request = new BillingProfileRequestModel().id(BILLING_PROFILE_ID);
    step = new CreateProfileJournalEntryStep(journalService, TEST_USER, request);
    when(flightContext.getFlightId()).thenReturn(FLIGHT_ID);
  }

  @Test
  void testDoAndUndoStep() throws InterruptedException {
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(journalService)
        .recordCreate(
            TEST_USER,
            BILLING_PROFILE_ID,
            IamResourceType.SPEND_PROFILE,
            "Billing profile created.",
            getFlightInformationOfInterest(flightContext),
            true);
    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    verify(journalService).removeJournalEntriesByFlightId(FLIGHT_ID);
  }
}
