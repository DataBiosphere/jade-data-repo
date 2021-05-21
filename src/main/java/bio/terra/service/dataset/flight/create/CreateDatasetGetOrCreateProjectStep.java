package bio.terra.service.dataset.flight.create;

import bio.terra.app.model.GoogleRegion;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.exception.GoogleProjectNamingException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;

import java.util.UUID;

public class CreateDatasetGetOrCreateProjectStep implements Step {
    private final ResourceService resourceService;

    public CreateDatasetGetOrCreateProjectStep(ResourceService resourceService) {
        this.resourceService = resourceService;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        FlightMap workingMap = context.getWorkingMap();
        BillingProfileModel profileModel = workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
        GoogleRegion region = DatasetJsonConversion.getRegionFromDatasetRequestModel(datasetRequestModel);
        // Since we find projects by their names, this is idempotent. If this step fails and is rerun,
        // Either the project will have been created and we will find it, or we will create it.
        UUID projectResourceId;
        try {
            projectResourceId = resourceService.getOrCreateDatasetProject(profileModel, region);
        } catch (GoogleProjectNamingException e) {
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, e);
        }
        workingMap.put(DatasetWorkingMapKeys.PROJECT_RESOURCE_ID, projectResourceId);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) throws InterruptedException {
        // At this time we do not delete projects, so no undo
        return StepResult.getStepResultSuccess();
    }

}

