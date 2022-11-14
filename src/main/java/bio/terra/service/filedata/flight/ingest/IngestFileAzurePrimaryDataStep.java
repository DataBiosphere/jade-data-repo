package bio.terra.service.filedata.flight.ingest;

import static bio.terra.service.filedata.DrsService.getLastNameFromPath;

import bio.terra.common.FlightUtils;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.FileLoadModel;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.configuration.ConfigEnum;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FSFileInfo;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class IngestFileAzurePrimaryDataStep implements Step {
  private static final Logger logger =
      LoggerFactory.getLogger(IngestFileAzurePrimaryDataStep.class);

  private final ConfigurationService configService;
  private final AzureBlobStorePdao azureBlobStorePdao;
  private final AuthenticatedUserRequest userRequest;
  private final Dataset dataset;

  public IngestFileAzurePrimaryDataStep(
      Dataset dataset,
      AzureBlobStorePdao azureBlobStorePdao,
      ConfigurationService configService,
      AuthenticatedUserRequest userRequest) {
    this.configService = configService;
    this.azureBlobStorePdao = azureBlobStorePdao;
    this.userRequest = userRequest;
    this.dataset = dataset;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    FileLoadModel fileLoadModel =
        context.getInputParameters().get(JobMapKeys.REQUEST.getKeyName(), FileLoadModel.class);

    FlightMap workingMap = context.getWorkingMap();

    String fileId = null;
    if (!dataset.isPredictableFileIds()) {
      fileId = workingMap.get(FileMapKeys.FILE_ID, String.class);
    }
    Boolean loadComplete = workingMap.get(FileMapKeys.LOAD_COMPLETED, Boolean.class);
    if (loadComplete == null || !loadComplete) {
      BillingProfileModel billingProfileModel =
          FlightUtils.getContextValue(
              context, ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
      AzureStorageAccountResource storageAccountResource =
          FlightUtils.getContextValue(
              context,
              CommonMapKeys.DATASET_STORAGE_ACCOUNT_RESOURCE,
              AzureStorageAccountResource.class);

      FSFileInfo fsFileInfo;
      if (configService.testInsertFault(ConfigEnum.LOAD_SKIP_FILE_LOAD)) {
        fsFileInfo =
            FSFileInfo.getTestInstance(fileId, storageAccountResource.getResourceId().toString());
      } else {
        fsFileInfo =
            azureBlobStorePdao.copyFile(
                dataset,
                billingProfileModel,
                fileLoadModel,
                fileId,
                storageAccountResource,
                userRequest);
      }
      if (fileId == null) {
        workingMap.put(FileMapKeys.FILE_ID, fsFileInfo.getFileId());
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
    AzureStorageAccountResource storageAccountResource =
        FlightUtils.getContextValue(
            context,
            CommonMapKeys.DATASET_STORAGE_ACCOUNT_RESOURCE,
            AzureStorageAccountResource.class);
    String fileName = getLastNameFromPath(fileLoadModel.getSourcePath());
    if (!azureBlobStorePdao.deleteDataFileById(
        fileId, fileName, storageAccountResource, userRequest)) {
      logger.warn(
          "File {} {} in storage account {} was not deleted.  It could ne non-existent",
          fileId,
          fileName,
          storageAccountResource.getName());
    }

    return StepResult.getStepResultSuccess();
  }
}
