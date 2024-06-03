package bio.terra.service.profile.azure;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.flight.create.CreateProfileManagedResourceGroup;
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
public class CreateProfileManagedResourceGroupStepTest {
  @Mock private ProfileService profileService;
  @Mock private FlightContext flightContext;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticationFixtures.randomUserRequest();

  private static final UUID BILLING_PROFILE_ID = UUID.randomUUID();

  private CreateProfileManagedResourceGroup step;

  @BeforeEach
  void setup() {
    BillingProfileRequestModel request = new BillingProfileRequestModel().id(BILLING_PROFILE_ID);
    step = new CreateProfileManagedResourceGroup(profileService, request, TEST_USER);
  }

  @Test
  void testDoAndUndoStep() throws InterruptedException {
    StepResult doResult = step.doStep(flightContext);
    assertThat(doResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
    StepResult undoResult = step.undoStep(flightContext);
    assertThat(undoResult.getStepStatus(), equalTo(StepStatus.STEP_RESULT_SUCCESS));
  }
}
