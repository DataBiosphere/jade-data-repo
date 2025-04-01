package bio.terra.service.profile.flight;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.rawls.RawlsService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.util.UUID;

public class AuthorizeRawlsBillingProjectsUseStep extends DefaultUndoStep {
  private final RawlsService rawlsService;
  private final UUID profileId;
  private final AuthenticatedUserRequest user;

  public AuthorizeRawlsBillingProjectsUseStep(
      RawlsService rawlsService, UUID profileId, AuthenticatedUserRequest user) {
    this.rawlsService = rawlsService;
    this.profileId = profileId;
    this.user = user;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    rawlsService.authorizeBillingProjectLink(profileId, user);
    return StepResult.getStepResultSuccess();
  }
}
