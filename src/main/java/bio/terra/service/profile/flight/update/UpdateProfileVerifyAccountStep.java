package bio.terra.service.profile.flight.update;

import bio.terra.model.BillingProfileRequestModel;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.profile.ProfileService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;


public class UpdateProfileVerifyAccountStep implements Step {
    private final ProfileService profileService;
    private final BillingProfileRequestModel request;
    private final AuthenticatedUserRequest user;

    public UpdateProfileVerifyAccountStep(ProfileService profileService,
                                      BillingProfileRequestModel request,
                                      AuthenticatedUserRequest user) {
        this.profileService = profileService;
        this.request = request;
        this.user = user;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        profileService.verifyAccount(request, user);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) throws InterruptedException {
        // Verify account has no side effects to clean up
        return StepResult.getStepResultSuccess();
    }
}

