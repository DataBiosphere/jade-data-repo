package bio.terra.service.profile.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.exception.ProfileNotFoundException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class LocateBillingInfoStepTest {
  @Mock private FlightContext context;
  @Mock private ProfileService profileService;
  private LocateBillingInfoStep step;
  private FlightMap workingMap;
  private final UUID PROFILE_ID = UUID.randomUUID();

  @BeforeEach
  void beforeEach() {
    step = new LocateBillingInfoStep(profileService, PROFILE_ID);
    workingMap = new FlightMap();
  }

  @Test
  void doStepTDRBilling() {
    when(context.getWorkingMap()).thenReturn(workingMap);
    StepResult result = step.doStep(context);

    verify(profileService).getProfileByIdNoCheck(PROFILE_ID);
    boolean workingMapValue =
        workingMap.get(ProfileMapKeys.TDR_BILLING_PROFILE_FALLBACK, Boolean.class);
    assertTrue(workingMapValue);
    assertThat("Step result is successful", StepResult.getStepResultSuccess(), equalTo(result));
  }

  @Test
  void doStepRawlsBilling() {
    when(context.getWorkingMap()).thenReturn(workingMap);
    doThrow(new ProfileNotFoundException("profile not found"))
        .when(profileService)
        .getProfileByIdNoCheck(PROFILE_ID);
    StepResult result = step.doStep(context);

    boolean workingMapValue =
        workingMap.get(ProfileMapKeys.TDR_BILLING_PROFILE_FALLBACK, Boolean.class);
    assertFalse(workingMapValue);
    assertThat("Step result is successful", StepResult.getStepResultSuccess(), equalTo(result));
  }
}
