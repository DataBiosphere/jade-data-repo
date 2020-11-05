package bio.terra.service.profile.flight.delete;

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
        profileService.deleteProfileIamResource(profileId);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) throws InterruptedException {
        return StepResult.getStepResultSuccess();
    }
}
