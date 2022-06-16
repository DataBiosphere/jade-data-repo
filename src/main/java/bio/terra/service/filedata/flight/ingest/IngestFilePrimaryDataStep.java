package bio.terra.service.filedata.flight.ingest;

import static bio.terra.service.filedata.DrsService.getLastNameFromPath;

import bio.terra.common.FlightUtils;
import bio.terra.model.FileLoadModel;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FSFileInfo;
import bio.terra.service.filedata.exception.InvalidUserProjectException;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;

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
      FSFileInfo fsFileInfo;
      try {
        if (dataset.isSelfHosted()) {
          fsFileInfo =
              gcsPdao.linkSelfHostedFile(
                  fileLoadModel, fileId, dataset.getProjectResource().getGoogleProjectId());
        } else {
          GoogleBucketResource bucketResource =
              FlightUtils.getContextValue(
                  context, FileMapKeys.BUCKET_INFO, GoogleBucketResource.class);
          if (configService.testInsertFault(ConfigEnum.LOAD_SKIP_FILE_LOAD)) {
            fsFileInfo =
                FSFileInfo.getTestInstance(fileId, bucketResource.getResourceId().toString());
          } else {
            fsFileInfo = gcsPdao.copyFile(dataset, fileLoadModel, fileId, bucketResource);
          }
        }
        workingMap.put(FileMapKeys.FILE_INFO, fsFileInfo);
      } catch (InvalidUserProjectException ex) {
        // We retry this exception because often when we've seen this error it has been transient
        // and untruthful -- i.e. the user project specified exists and has a legal id.
        return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, ex);
      }
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
    GoogleBucketResource bucketResource =
        FlightUtils.getContextValue(context, FileMapKeys.BUCKET_INFO, GoogleBucketResource.class);
    String fileName = getLastNameFromPath(fileLoadModel.getSourcePath());
    gcsPdao.deleteFileById(dataset, fileId, fileName, bucketResource);

    return StepResult.getStepResultSuccess();
  }
}
