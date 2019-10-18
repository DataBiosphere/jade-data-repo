package bio.terra.service.filedata.flight.delete;

import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.DataLocationService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class DeleteFilePrimaryDataStep implements Step {
    private static final Logger logger = LoggerFactory.getLogger(DeleteFilePrimaryDataStep.class);

    private final Dataset dataset;
    private final String fileId;
    private final GcsPdao gcsPdao;
    private final FireStoreDao fileDao;
    private final DataLocationService locationService;

    public DeleteFilePrimaryDataStep(Dataset dataset,
                                     String fileId,
                                     GcsPdao gcsPdao,
                                     FireStoreDao fileDao,
                                     DataLocationService locationService) {
        this.dataset = dataset;
        this.fileId = fileId;
        this.gcsPdao = gcsPdao;
        this.fileDao = fileDao;
        this.locationService = locationService;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        FireStoreFile fireStoreFile = workingMap.get(FileMapKeys.FIRESTORE_FILE, FireStoreFile.class);
        if (fireStoreFile != null) {
            GoogleBucketResource bucketResource = locationService.lookupBucket(fireStoreFile.getBucketResourceId());
            gcsPdao.deleteFileByGspath(fireStoreFile.getGspath(), bucketResource);
        }
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // No undo is possible - the file either still exists or it doesn't
        return StepResult.getStepResultSuccess();
    }

}
