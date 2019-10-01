package bio.terra.flight.file.ingest;

import bio.terra.filedata.google.FireStoreDao;
import bio.terra.flight.file.FileMapKeys;
import bio.terra.metadata.Dataset;
import bio.terra.model.FileLoadModel;
import bio.terra.resourcemanagement.metadata.google.GoogleBucketResource;
import bio.terra.service.JobMapKeys;
import bio.terra.service.dataproject.DataLocationService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

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
        FlightMap inputParameters = context.getInputParameters();
        FileLoadModel fileLoadModel = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), FileLoadModel.class);
        GoogleBucketResource bucketForFile = locationService.getBucketForFile(fileLoadModel.getProfileId());
        FlightMap workingMap = context.getWorkingMap();
        workingMap.put(FileMapKeys.BUCKET_INFO, bucketForFile);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // There is not much to undo here. It is possible that a bucket was created in the last step. We could look to
        // see if there are no other files in the bucket and delete it here, but I think it is likely the bucket will
        // be used again.
        return StepResult.getStepResultSuccess();
    }
}
