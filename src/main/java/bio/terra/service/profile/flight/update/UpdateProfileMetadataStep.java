package bio.terra.service.profile.flight.update;

import bio.terra.model.BillingProfileModel;
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
import org.springframework.http.HttpStatus;

public class UpdateProfileMetadataStep implements Step {

    private final ProfileService profileService;
    private final BillingProfileRequestModel profileRequest;
    private final AuthenticatedUserRequest user;
    private static final Logger logger = LoggerFactory.getLogger(UpdateProfileMetadataStep.class);

    public UpdateProfileMetadataStep(ProfileService profileService,
                                     BillingProfileRequestModel profileRequest,
                                     AuthenticatedUserRequest user) {
        this.profileService = profileService;
        this.profileRequest = profileRequest;
        this.user = user;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        // get current billing model so we know what to revert to
        BillingProfileModel oldProfileModel = profileService.getProfileById(profileRequest.getId(), user);
        FlightMap workingMap = context.getWorkingMap();
        workingMap.put(JobMapKeys.REVERT_TO.getKeyName(), oldProfileModel);

        // Update to new billing metadata
        BillingProfileModel profileModel = profileService.updateProfileMetadata(profileRequest);
        workingMap.put(JobMapKeys.RESPONSE.getKeyName(), profileModel);
        workingMap.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.CREATED);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        logger.info("Profile update failed. Reverting to old profile metadata.");
        FlightMap workingMap = context.getWorkingMap();
        BillingProfileModel oldProfileModel =
            workingMap.get(JobMapKeys.REVERT_TO.getKeyName(), BillingProfileModel.class);
        BillingProfileRequestModel requestOldModel = new BillingProfileRequestModel()
            .id(oldProfileModel.getId())
            .biller(oldProfileModel.getBiller())
            .billingAccountId(oldProfileModel.getBillingAccountId())
            .description(oldProfileModel.getDescription())
            .profileName(oldProfileModel.getProfileName());
        profileService.updateProfileMetadata(requestOldModel);
        return StepResult.getStepResultSuccess();
    }
}
