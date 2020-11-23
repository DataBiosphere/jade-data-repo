package bio.terra.service.profile.flight.update;

import bio.terra.model.BillingProfileRequestModel;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.ProfileService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class UpdateProfileAuthzIamStep implements Step {
    private static final Logger logger = LoggerFactory.getLogger(UpdateProfileAuthzIamStep.class);
    private final ProfileService profileService;
    private final BillingProfileRequestModel request;
    private final AuthenticatedUserRequest user;

    public UpdateProfileAuthzIamStep(ProfileService profileService,
                                     BillingProfileRequestModel request,
                                     AuthenticatedUserRequest user) {

        this.profileService = profileService;
        this.request = request;
        this.user = user;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        profileService.updateProfileIamResource(request, user);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) throws InterruptedException {
        logger.info("Profile update failed. Reverting to old profile metadata for SAM resource.");
        FlightMap workingMap = context.getWorkingMap();
        bio.terra.model.BillingProfileModel oldProfileModel =
            workingMap.get(JobMapKeys.REVERT_TO.getKeyName(), bio.terra.model.BillingProfileModel.class);
        BillingProfileRequestModel requestOldModel = new BillingProfileRequestModel()
            .id(oldProfileModel.getId())
            .biller(oldProfileModel.getBiller())
            .billingAccountId(oldProfileModel.getBillingAccountId())
            .description(oldProfileModel.getDescription())
            .profileName(oldProfileModel.getProfileName());
        profileService.updateProfileIamResource(requestOldModel, user);
        return StepResult.getStepResultSuccess();
    }
}
