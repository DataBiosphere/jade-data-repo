package bio.terra.service.dataset.flight.delete;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.Objects;
import java.util.UUID;
import org.springframework.http.HttpStatus;

public class DeleteDatasetAzurePrimaryDataStep extends DefaultUndoStep {

  private final AzureBlobStorePdao azureBlobStorePdao;
  private final TableDao tableDao;
  private final UUID datasetId;
  private final AuthenticatedUserRequest userRequest;

  public DeleteDatasetAzurePrimaryDataStep(
      AzureBlobStorePdao azureBlobStorePdao,
      TableDao tableDao,
      UUID datasetId,
      AuthenticatedUserRequest userRequest) {
    this.azureBlobStorePdao = azureBlobStorePdao;
    this.tableDao = tableDao;
    this.datasetId = datasetId;
    this.userRequest = userRequest;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap map = context.getWorkingMap();
    AzureStorageAuthInfo storageAuthInfo =
        Objects.requireNonNull(
            map.get(CommonMapKeys.DATASET_STORAGE_AUTH_INFO, AzureStorageAuthInfo.class),
            "No Azure storage auth info found");

    tableDao.deleteFilesFromDataset(
        storageAuthInfo, datasetId, f -> azureBlobStorePdao.deleteFile(f, userRequest));

    map.put(JobMapKeys.STATUS_CODE.getKeyName(), HttpStatus.NO_CONTENT);
    return StepResult.getStepResultSuccess();
  }
}
