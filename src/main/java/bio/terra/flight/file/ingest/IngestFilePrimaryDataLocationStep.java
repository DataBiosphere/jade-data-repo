package bio.terra.flight.file.ingest;

import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.filesystem.exception.FileSystemCorruptException;
import bio.terra.flight.file.FileMapKeys;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.FSFile;
import bio.terra.metadata.FSObjectBase;
import bio.terra.metadata.FSObjectType;
import bio.terra.resourcemanagement.metadata.google.GoogleBucketResource;
import bio.terra.service.dataproject.DataLocationService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.UUID;

public class IngestFilePrimaryDataLocationStep implements Step {
    private final FireStoreFileDao fileDao;
    private final Dataset dataset;
    private final DataLocationService locationService;

    public IngestFilePrimaryDataLocationStep(FireStoreFileDao fileDao,
                                             Dataset dataset,
                                             DataLocationService locationService) {
        this.fileDao = fileDao;
        this.dataset = dataset;
        this.locationService = locationService;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        UUID objectId = UUID.fromString(workingMap.get(FileMapKeys.OBJECT_ID, String.class));
        FSObjectBase fsObject = fileDao.retrieve(dataset, objectId);
        if (fsObject.getObjectType() != FSObjectType.INGESTING_FILE) {
            throw new FileSystemCorruptException("This should be a file!");
        }

        GoogleBucketResource bucketForFile = locationService.getBucketForFile((FSFile) fsObject);
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
