package bio.terra.service.profile.flight.create;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentResource;
import bio.terra.service.resourcemanagement.azure.AzureApplicationDeploymentService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.List;
import java.util.UUID;

public class CreateProfileRegisterDeployedApplicationStep implements Step {
  private final AzureApplicationDeploymentService applicationDeploymentService;

  /**
   * This step registers an association between the billing profile created by this flight and its
   * {@link AzureApplicationDeploymentResource}, and makes it available in its working map.
   */
  public CreateProfileRegisterDeployedApplicationStep(
      AzureApplicationDeploymentService applicationDeploymentService) {
    this.applicationDeploymentService = applicationDeploymentService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    BillingProfileModel billingProfile = getBillingProfile(context);
    final AzureApplicationDeploymentResource applicationDeployment =
        applicationDeploymentService.getOrRegisterApplicationDeployment(billingProfile);
    context
        .getWorkingMap()
        .put(ProfileMapKeys.PROFILE_APPLICATION_DEPLOYMENT, applicationDeployment);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    UUID profileId = getBillingProfile(context).getId();
    // We unconditionally delete any registered association between this billing profile and its
    // application deployment when undoing this step:
    // there is no circumstance where an association would already exist and be exempt from clean up
    // because an application deployment can be associated with at most one billing profile.
    List<UUID> appIdList =
        applicationDeploymentService.markUnusedApplicationDeploymentsForDelete(profileId);
    applicationDeploymentService.deleteApplicationDeploymentMetadata(appIdList);
    return StepResult.getStepResultSuccess();
  }

  /**
   * @return the {@link BillingProfileModel} obtained from the working map, assumed to already be
   *     written as the job's response.
   */
  private BillingProfileModel getBillingProfile(FlightContext context) {
    return context.getWorkingMap().get(JobMapKeys.RESPONSE.getKeyName(), BillingProfileModel.class);
  }
}
