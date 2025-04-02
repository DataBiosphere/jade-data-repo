package bio.terra.service.profile.flight;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class PerformRawlsBillingProjectStepTest {
  @Mock private FlightContext flightContext;
  @Mock private Step step;

  @Test
  void isEnabled() throws InterruptedException {
    var flightMap = new FlightMap();
    flightMap.put(ProfileMapKeys.TDR_BILLING_PROFILE_FALLBACK, false);
    when(flightContext.getWorkingMap()).thenReturn(flightMap);
    // Strict mocking shows that we run the nested "doStep" when the optional step is enabled
    when(step.doStep(flightContext)).thenReturn(StepResult.getStepResultSuccess());
    PerformRawlsBillingProjectStep optionalStep = new PerformRawlsBillingProjectStep(this.step);
    optionalStep.doStep(flightContext);
    assertTrue(optionalStep.isEnabled(flightContext));
  }

  @Test
  void isDisabled() throws InterruptedException {
    var flightMap = new FlightMap();
    flightMap.put(ProfileMapKeys.TDR_BILLING_PROFILE_FALLBACK, true);
    when(flightContext.getWorkingMap()).thenReturn(flightMap);
    PerformRawlsBillingProjectStep optionalStep = new PerformRawlsBillingProjectStep(this.step);
    optionalStep.doStep(flightContext);
    assertFalse(optionalStep.isEnabled(flightContext));
  }
}
