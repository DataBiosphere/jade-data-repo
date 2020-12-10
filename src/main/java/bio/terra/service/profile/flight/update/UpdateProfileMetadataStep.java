package bio.terra.service.profile.flight.update;

import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileUpdateModel;
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
    private final BillingProfileUpdateModel profileRequest;
    private final AuthenticatedUserRequest user;
    private static final Logger logger = LoggerFactory.getLogger(UpdateProfileMetadataStep.class);

    public UpdateProfileMetadataStep(ProfileService profileService,
                                     BillingProfileUpdateModel profileRequest,
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

        // TODO: Is there a better way to handle this?
        // I want to allow for just updating a description or just updating the billing account
        // build request - handle if description or billing account id is not included
        if (profileRequest.getBillingAccountId() == null || profileRequest.getBillingAccountId().isEmpty()) {
            profileRequest.setBillingAccountId(oldProfileModel.getBillingAccountId());
        }
        if (profileRequest.getDescription() == null || profileRequest.getDescription().isEmpty()) {
            profileRequest.setDescription(oldProfileModel.getDescription());
        }

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
        BillingProfileUpdateModel requestOldModel = new BillingProfileUpdateModel()
            .id(oldProfileModel.getId())
            .billingAccountId(oldProfileModel.getBillingAccountId())
            .description(oldProfileModel.getDescription());
        profileService.updateProfileMetadata(requestOldModel);
        return StepResult.getStepResultSuccess();
    }
}
