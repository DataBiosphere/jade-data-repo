package bio.terra.service.upgrade.flight;

import bio.terra.model.UpgradeModel;
import bio.terra.model.UpgradeResponseModel;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.ProfileService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class UpgradeProfileResourcesStep implements Step {
    private final ProfileService profileService;
    private final UpgradeModel request;
    private final AuthenticatedUserRequest user;

    public UpgradeProfileResourcesStep(ProfileService profileService,
                                       UpgradeModel request,
                                       AuthenticatedUserRequest user) {
        this.profileService = profileService;
        this.request = request;
        this.user = user;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        UpgradeResponseModel response = profileService.upgradeProfileResources(request, user);
        context.getWorkingMap().put(JobMapKeys.RESPONSE.getKeyName(), response);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) throws InterruptedException {
        return StepResult.getStepResultSuccess();
    }
}
