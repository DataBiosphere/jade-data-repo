package bio.terra.service.profile.flight;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.profile.google.GoogleBillingService;
import bio.terra.service.resourcemanagement.exception.InaccessibleBillingAccountException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

/**
 * This step ensures that the billing account added to the working map by the
 * AuthorizeBillingProfileUseStep remains enabled and is accessible to the data repo.
 */
public class VerifyBillingAccountAccessStep implements Step {
  private final GoogleBillingService googleBillingService;

  public VerifyBillingAccountAccessStep(GoogleBillingService googleBillingService) {
    this.googleBillingService = googleBillingService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    BillingProfileModel profileModel =
        workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
    String billingAccountId = profileModel.getBillingAccountId();
    //  TODO: check bill account usable and validate delegation path
    if (!googleBillingService.repositoryCanAccess(billingAccountId)) {
      throw new InaccessibleBillingAccountException(
          "The repository needs access to billing account "
              + billingAccountId
              + " to perform the requested operation");
    }
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    // Verify account has no side effects to clean up
    return StepResult.getStepResultSuccess();
  }
}
