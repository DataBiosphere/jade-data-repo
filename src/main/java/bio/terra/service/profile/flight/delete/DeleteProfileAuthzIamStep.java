package bio.terra.service.profile.flight.delete;

import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.ProfileService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class DeleteProfileAuthzIamStep implements Step {
    private final ProfileService profileService;
    private final String profileId;

    public DeleteProfileAuthzIamStep(ProfileService profileService,
                                     String profileId) {
        this.profileService = profileService;
        this.profileId = profileId;

    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        AuthenticatedUserRequest user = context.getInputParameters().get(
            JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

        profileService.deleteProfileIamResource(profileId, user);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) throws InterruptedException {
        return StepResult.getStepResultSuccess();
    }
}
