package bio.terra.flight.study.create;

import bio.terra.flight.exception.InaccessibleBillingAccountException;
import bio.terra.metadata.BillingProfile;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.StudyRequestModel;
import bio.terra.service.JobMapKeys;
import bio.terra.service.ProfileService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.UUID;

public class CreateStudyCloudResourcesStep implements Step {

    private ProfileService profileService;

    public CreateStudyCloudResourcesStep(ProfileService profileService) {
        this.profileService = profileService;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        StudyRequestModel studyRequest = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), StudyRequestModel.class);
        UUID defaultBillingProfileId = UUID.fromString(studyRequest.getDefaultProfile());
        FlightMap workingMap = context.getWorkingMap();
        UUID studyId = workingMap.get("studyId", UUID.class);
        BillingProfileModel defaultProfile = profileService.getProfileById(defaultBillingProfileId);
        if (!defaultProfile.isAccessible()) {
            throw new InaccessibleBillingAccountException("Billing account of the default profile is inaccessible.");
        }
        // resourceService.
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
    }
}

