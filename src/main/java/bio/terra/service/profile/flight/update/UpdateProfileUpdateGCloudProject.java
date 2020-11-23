package bio.terra.service.profile.flight.update;

import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class UpdateProfileUpdateGCloudProject implements Step {
    private static final Logger logger = LoggerFactory.getLogger(UpdateProfileUpdateGCloudProject.class);
    private final ProfileService profileService;
    private final GoogleProjectService googleProjectService;
    private final BillingProfileRequestModel request;
    private final AuthenticatedUserRequest user;

    public UpdateProfileUpdateGCloudProject(ProfileService profileService,
                                            GoogleProjectService projectService,
                                            BillingProfileRequestModel request,
                                            AuthenticatedUserRequest user) {
        this.profileService = profileService;
        this.googleProjectService = projectService;
        this.request = request;
        this.user = user;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        // Check if billing profile id is diff from existing
        FlightMap workingMap = context.getWorkingMap();
        BillingProfileModel existingBillingProfileModel =
            workingMap.get(JobMapKeys.REVERT_TO.getKeyName(), BillingProfileModel.class);
        // maybe I should check the actual google project billing account instead?
        // This is an okay start
        if (existingBillingProfileModel.getBillingAccountId() != request.getBillingAccountId()) {
            logger.info("Updating billing profile id {} in google project", request.getBillingAccountId());
            profileService.verifyAccount(request, user);
            // use GoogleProjectService new method to update billing account
            // should this also live in profileService?
            googleProjectService.updateBillingProfile(request, user);
        } else {
            logger.info("Billing profile id already set to {}", request.getBillingAccountId());
        }
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) throws InterruptedException {
        //TODO: Revert project id change
        return StepResult.getStepResultSuccess();
    }
}

