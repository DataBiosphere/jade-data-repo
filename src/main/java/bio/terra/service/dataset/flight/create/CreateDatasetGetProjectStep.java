package bio.terra.service.dataset.flight.create;

import bio.terra.buffer.model.HandoutRequestBody;
import bio.terra.buffer.model.ResourceInfo;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.resourcemanagement.BufferService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.UUID;


public class CreateDatasetGetProjectStep implements Step {
    private final BufferService bufferService;

    public CreateDatasetGetProjectStep(BufferService bufferService) {
        this.bufferService = bufferService;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        // Requests a google project from RBS and puts it in the working map
        FlightMap workingMap = context.getWorkingMap();
        String handoutRequestId = UUID.randomUUID().toString();
        HandoutRequestBody request = new HandoutRequestBody().handoutRequestId(handoutRequestId);
        ResourceInfo resource = bufferService.handoutResource(request);
        String projectId = resource.getCloudResourceUid().getGoogleProjectUid().getProjectId();
        workingMap.put(DatasetWorkingMapKeys.GOOGLE_PROJECT_ID, projectId);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }
}

