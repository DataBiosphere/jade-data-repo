package bio.terra.service.filedata.flight.ingest;

import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.model.FileLoadModel;
import bio.terra.service.resourcemanagement.exception.BucketLockException;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.DataLocationService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;

public class IngestFilePrimaryDataLocationStep implements Step {
    private final FireStoreDao fileDao;
    private final Dataset dataset;
    private final DataLocationService locationService;

    public IngestFilePrimaryDataLocationStep(FireStoreDao fileDao,
                                             Dataset dataset,
                                             DataLocationService locationService) {
        this.fileDao = fileDao;
        this.dataset = dataset;
        this.locationService = locationService;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        Boolean loadComplete = workingMap.get(FileMapKeys.LOAD_COMPLETED, Boolean.class);
        if (loadComplete == null || !loadComplete) {
            FlightMap inputParameters = context.getInputParameters();
            FileLoadModel fileLoadModel = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), FileLoadModel.class);
            try {
                GoogleBucketResource bucketForFile =
                    locationService.getOrCreateBucketForFile(fileLoadModel.getProfileId(), context.getFlightId());
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

        // get the bucket name from the profile id
        FlightMap inputParameters  = context.getInputParameters();
        FileLoadModel fileLoadModel = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), FileLoadModel.class);

        // update the bucket metadata, to match the state of the cloud resource
        locationService.updateBucketMetadata(fileLoadModel.getProfileId(), context.getFlightId());

        return StepResult.getStepResultSuccess();
    }
}
