package bio.terra.service.profile.flight.create;

import bio.terra.model.BillingProfileRequestModel;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;

public class GetOrCreateProfileIdStep implements Step {
  private final BillingProfileRequestModel request;

  public GetOrCreateProfileIdStep(BillingProfileRequestModel request) {
    this.request = request;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    UUID profileId = request.getId();
    if (profileId == null) {
      request.setId(UUID.randomUUID());
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
