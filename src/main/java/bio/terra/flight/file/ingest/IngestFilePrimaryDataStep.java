package bio.terra.flight.file.ingest;

import bio.terra.filesystem.FireStoreFileDao;
import bio.terra.filesystem.exception.FileSystemCorruptException;
import bio.terra.flight.file.FileMapKeys;
import bio.terra.metadata.FSFile;
import bio.terra.metadata.FSFileInfo;
import bio.terra.metadata.FSObjectBase;
import bio.terra.metadata.FSObjectType;
import bio.terra.metadata.Dataset;
import bio.terra.model.FileLoadModel;
import bio.terra.pdao.gcs.GcsPdao;
import bio.terra.resourcemanagement.metadata.google.GoogleBucketResource;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.UUID;

public class IngestFilePrimaryDataStep implements Step {
    private final FireStoreFileDao fileDao;
    private final GcsPdao gcsPdao;
    private final Dataset dataset;

    public IngestFilePrimaryDataStep(FireStoreFileDao fileDao,
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
        UUID objectId = UUID.fromString(workingMap.get(FileMapKeys.OBJECT_ID, String.class));
        FSObjectBase fsObject = fileDao.retrieve(dataset, objectId);
        if (fsObject.getObjectType() != FSObjectType.INGESTING_FILE) {
            throw new FileSystemCorruptException("This should be a file!");
        }

        // In the previous step a bucket was selected for this file to go into and stored in the working map. Here, we
        // store the bucket resource id on the fsFile metadata to let the gcsPdao know where to copy the file.
        GoogleBucketResource bucketResource = workingMap.get(FileMapKeys.BUCKET_INFO, GoogleBucketResource.class);
        FSFile fsFile = (FSFile) fsObject;
        fsFile.bucketResourceId(bucketResource.getResourceId().toString());

        FSFileInfo fsFileInfo = gcsPdao.copyFile(dataset, fileLoadModel, fsFile);
        workingMap.put(FileMapKeys.FILE_INFO, fsFileInfo);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        String objectId = workingMap.get(FileMapKeys.OBJECT_ID, String.class);
        FSObjectBase fsObject = fileDao.retrieve(dataset, UUID.fromString(objectId));
        if (fsObject.getObjectType() == FSObjectType.DIRECTORY) {
            throw new FileSystemCorruptException("This should be a file!");
        }

        GoogleBucketResource bucketResource = workingMap.get(FileMapKeys.BUCKET_INFO, GoogleBucketResource.class);
        FSFile fsFile = (FSFile) fsObject;
        fsFile.bucketResourceId(bucketResource.getResourceId().toString());

        gcsPdao.deleteFile(fsFile);
        return StepResult.getStepResultSuccess();
    }

}
