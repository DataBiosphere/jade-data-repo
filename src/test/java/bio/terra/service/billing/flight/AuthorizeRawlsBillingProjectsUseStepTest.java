package bio.terra.service.billing.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.instanceOf;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.profile.exception.BillingProjectNotAccessibleException;
import bio.terra.service.profile.flight.AuthorizeRawlsBillingProjectsUseStep;
import bio.terra.service.rawls.RawlsService;
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
class AuthorizeRawlsBillingProjectsUseStepTest {
  @Mock private FlightContext context;
  @Mock private RawlsService rawlsService;
  private AuthorizeRawlsBillingProjectsUseStep step;
  private static final UUID PROFILE_ID = UUID.randomUUID();
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  @BeforeEach
  void beforeEach() {
    step = new AuthorizeRawlsBillingProjectsUseStep(rawlsService, PROFILE_ID, TEST_USER);
  }

  @Test
  void doStep() {
    StepResult result = step.doStep(context);
    verify(rawlsService).authorizeBillingProjectLink(PROFILE_ID, TEST_USER);
    assertThat("Successful step result", result, equalTo(StepResult.getStepResultSuccess()));
  }

  @Test
  void doStep_NotAccessible() {
    doThrow(new BillingProjectNotAccessibleException("Billing project not accessible"))
        .when(rawlsService)
        .authorizeBillingProjectLink(PROFILE_ID, TEST_USER);
    StepResult result = step.doStep(context);
    assertThat(
        "Fatal step result", result.getStepStatus(), equalTo(StepStatus.STEP_RESULT_FAILURE_FATAL));
    assertThat(
        "Correct exception returned",
        result.getException().orElseThrow(),
        instanceOf(BillingProjectNotAccessibleException.class));
  }
}
