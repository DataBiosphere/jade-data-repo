package bio.terra.service.profile.flight;

import bio.terra.service.job.OptionalStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;

public class PerformTDRBillingStep extends OptionalStep {
  public PerformTDRBillingStep(Step step) {
    super(step);
  }

  @Override
  public boolean isEnabled(FlightContext context) {
    FlightMap map = context.getWorkingMap();
    boolean tdrBillingProfileFallback =
        map.get(ProfileMapKeys.TDR_BILLING_PROFILE_FALLBACK, boolean.class);
    return tdrBillingProfileFallback;
  }
}
