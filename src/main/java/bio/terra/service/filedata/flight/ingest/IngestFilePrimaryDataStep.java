package bio.terra.service.filedata.flight.ingest;

import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FSFileInfo;
import bio.terra.model.FileLoadModel;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.job.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

public class IngestFilePrimaryDataStep implements Step {
    private final FireStoreDao fileDao;
    private final GcsPdao gcsPdao;
    private final Dataset dataset;

    public IngestFilePrimaryDataStep(FireStoreDao fileDao,
                                     Dataset dataset,
                                     GcsPdao gcsPdao) {
        this.fileDao = fileDao;
        this.gcsPdao = gcsPdao;
        this.dataset = dataset;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        FileLoadModel fileLoadModel = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), FileLoadModel.class);

        FlightMap workingMap = context.getWorkingMap();
        String fileId = workingMap.get(FileMapKeys.FILE_ID, String.class);

        // In the previous step a bucket was selected for this file to go into and stored in the working map. Here, we
        // store the bucket resource id on the fsFile metadata to let the gcsPdao know where to copy the file.
        GoogleBucketResource bucketResource = workingMap.get(FileMapKeys.BUCKET_INFO, GoogleBucketResource.class);

        FSFileInfo fsFileInfo = gcsPdao.copyFile(dataset, fileLoadModel, fileId, bucketResource);
        workingMap.put(FileMapKeys.FILE_INFO, fsFileInfo);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        String fileId = workingMap.get(FileMapKeys.FILE_ID, String.class);
        GoogleBucketResource bucketResource = workingMap.get(FileMapKeys.BUCKET_INFO, GoogleBucketResource.class);

        gcsPdao.deleteFileById(dataset, fileId, bucketResource);

        return StepResult.getStepResultSuccess();
    }

}
