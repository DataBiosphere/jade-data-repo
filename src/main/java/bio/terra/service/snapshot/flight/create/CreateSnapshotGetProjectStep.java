package bio.terra.service.snapshot.flight.create;

import bio.terra.buffer.model.HandoutRequestBody;
import bio.terra.buffer.model.ResourceInfo;
import bio.terra.service.resourcemanagement.BufferService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.UUID;


public class CreateSnapshotGetProjectStep implements Step {
    private final BufferService bufferService;

    public CreateSnapshotGetProjectStep(BufferService bufferService) {
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
        workingMap.put(SnapshotWorkingMapKeys.GOOGLE_PROJECT_ID, projectId);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }
}

