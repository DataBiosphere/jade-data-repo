package bio.terra.datarepo.service.filedata.flight.ingest;

import static bio.terra.datarepo.service.filedata.DrsService.getLastNameFromPath;

import bio.terra.datarepo.model.FileLoadModel;
import bio.terra.datarepo.service.configuration.ConfigEnum;
import bio.terra.datarepo.service.configuration.ConfigurationService;
import bio.terra.datarepo.service.dataset.Dataset;
import bio.terra.datarepo.service.filedata.FSFileInfo;
import bio.terra.datarepo.service.filedata.flight.FileMapKeys;
import bio.terra.datarepo.service.filedata.google.gcs.GcsPdao;
import bio.terra.datarepo.service.job.JobMapKeys;
import bio.terra.datarepo.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.time.Instant;

public class IngestFilePrimaryDataStep implements Step {
  private final ConfigurationService configService;
  private final GcsPdao gcsPdao;
  private final Dataset dataset;

  public IngestFilePrimaryDataStep(
      Dataset dataset, GcsPdao gcsPdao, ConfigurationService configService) {
    this.configService = configService;
    this.gcsPdao = gcsPdao;
    this.dataset = dataset;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    FlightMap inputParameters = context.getInputParameters();
    FileLoadModel fileLoadModel =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), FileLoadModel.class);

    FlightMap workingMap = context.getWorkingMap();
    String fileId = workingMap.get(FileMapKeys.FILE_ID, String.class);
    Boolean loadComplete = workingMap.get(FileMapKeys.LOAD_COMPLETED, Boolean.class);
    if (loadComplete == null || !loadComplete) {
      GoogleBucketResource bucketResource = IngestUtils.getBucketInfo(context);
      FSFileInfo fsFileInfo;
      if (configService.testInsertFault(ConfigEnum.LOAD_SKIP_FILE_LOAD)) {
        fsFileInfo =
            new FSFileInfo()
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
    FlightMap inputParameters = context.getInputParameters();
    FileLoadModel fileLoadModel =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), FileLoadModel.class);
    FlightMap workingMap = context.getWorkingMap();
    String fileId = workingMap.get(FileMapKeys.FILE_ID, String.class);
    GoogleBucketResource bucketResource = IngestUtils.getBucketInfo(context);
    String fileName = getLastNameFromPath(fileLoadModel.getSourcePath());
    gcsPdao.deleteFileById(dataset, fileId, fileName, bucketResource);

    return StepResult.getStepResultSuccess();
  }
}
