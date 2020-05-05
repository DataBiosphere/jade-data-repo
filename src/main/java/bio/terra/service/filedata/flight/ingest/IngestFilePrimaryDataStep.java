package bio.terra.service.filedata.flight.ingest;

import bio.terra.model.FileLoadModel;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FSFileInfo;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.time.Instant;

public class IngestFilePrimaryDataStep implements Step {
    private final ConfigurationService configService;
    private final GcsPdao gcsPdao;
    private final Dataset dataset;

    public IngestFilePrimaryDataStep(Dataset dataset,
                                     GcsPdao gcsPdao,
                                     ConfigurationService configService) {
        this.configService = configService;
        this.gcsPdao = gcsPdao;
        this.dataset = dataset;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap inputParameters = context.getInputParameters();
        FileLoadModel fileLoadModel = inputParameters.get(JobMapKeys.REQUEST.getKeyName(), FileLoadModel.class);

        FlightMap workingMap = context.getWorkingMap();
        String fileId = workingMap.get(FileMapKeys.FILE_ID, String.class);
        Boolean loadComplete = workingMap.get(FileMapKeys.LOAD_COMPLETED, Boolean.class);
        if (loadComplete == null || !loadComplete) {
            // The bucket has been selected for this file. In the single file load case, the info
            // is stored in the working map. In the bulk load case, the info is stored in the input
            // parameters.
            GoogleBucketResource bucketResource =
                inputParameters.get(FileMapKeys.BUCKET_INFO, GoogleBucketResource.class);
            if (bucketResource == null) {
                bucketResource = workingMap.get(FileMapKeys.BUCKET_INFO, GoogleBucketResource.class);
            }

            FSFileInfo fsFileInfo;
            if (configService.testInsertFault(ConfigEnum.LOAD_SKIP_FILE_LOAD)) {
                fsFileInfo = new FSFileInfo()
                    .fileId(fileId)
                    .bucketResourceId(bucketResource.getResourceId().toString())
                    .checksumCrc32c(null)
                    .checksumMd5("baaaaaad")
                    .createdDate(Instant.now().toString())
                    .gspath("gs://path")
                    .size(100L);
            } else {
                fsFileInfo = gcsPdao.copyFile(dataset, fileLoadModel, fileId, bucketResource);
            }
            workingMap.put(FileMapKeys.FILE_INFO, fsFileInfo);
        }
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
