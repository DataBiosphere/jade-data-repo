package bio.terra.service.filedata.flight.ingest;

import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.resourcemanagement.DataLocationService;
import bio.terra.service.resourcemanagement.exception.BucketLockException;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;

public class IngestFilePrimaryDataLocationStep implements Step {
    private final DataLocationService locationService;
    private final String profileId;

    public IngestFilePrimaryDataLocationStep(DataLocationService locationService, String profileId) {
        this.locationService = locationService;
        this.profileId = profileId;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        FlightMap workingMap = context.getWorkingMap();
        Boolean loadComplete = workingMap.get(FileMapKeys.LOAD_COMPLETED, Boolean.class);
        if (loadComplete == null || !loadComplete) {
            try {
                GoogleBucketResource bucketForFile =
                    locationService.getOrCreateBucketForFile(profileId, context.getFlightId());
                workingMap.put(FileMapKeys.BUCKET_INFO, bucketForFile);
            } catch (BucketLockException blEx) {
                return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, blEx);
            }
        }
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // There is not much to undo here. It is possible that a bucket was created in the last step. We could look to
        // see if there are no other files in the bucket and delete it here, but I think it is likely the bucket will
        // be used again.
        locationService.updateBucketMetadata(profileId, context.getFlightId());
        return StepResult.getStepResultSuccess();
    }
}
