package bio.terra.service.snapshot.flight.create;

import bio.terra.common.FlightUtils;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.azure.data.tables.TableServiceClient;
import java.util.List;
import java.util.UUID;

public class CreateSnapshotStorageTableDataStep implements Step {
  private final TableDao tableDao;
  private final AzureAuthService azureAuthService;
  private final DatasetService datasetService;
  private final AzureSynapsePdao azureSynapsePdao;
  private final String datasetName;

  public CreateSnapshotStorageTableDataStep(
      TableDao tableDao,
      AzureAuthService azureAuthService,
      DatasetService datasetService,
      AzureSynapsePdao azureSynapsePdao,
      String datasetName) {
    this.tableDao = tableDao;
    this.azureAuthService = azureAuthService;
    this.datasetService = datasetService;
    this.azureSynapsePdao = azureSynapsePdao;
    this.datasetName = datasetName;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    // Dataset
    AzureStorageAuthInfo datasetStorageAuthInfo =
        FlightUtils.getContextValue(
            context, CommonMapKeys.DATASET_STORAGE_AUTH_INFO, AzureStorageAuthInfo.class);
    TableServiceClient datasetTableServiceClient =
        azureAuthService.getTableServiceClient(datasetStorageAuthInfo);
    Dataset dataset = datasetService.retrieveByName(datasetName);

    // Snapshot
    AzureStorageAuthInfo snapshotStorageAuthInfo =
        FlightUtils.getContextValue(
            context, CommonMapKeys.SNAPSHOT_STORAGE_AUTH_INFO, AzureStorageAuthInfo.class);
    TableServiceClient snapshotTableServiceClient =
        azureAuthService.getTableServiceClient(snapshotStorageAuthInfo);
    UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);

    // TODO - place for performance improvement
    List<String> refIds = azureSynapsePdao.getRefIdsForDataset(dataset);

    tableDao.addFilesToSnapshot(
        datasetTableServiceClient,
        snapshotTableServiceClient,
        dataset.getId(),
        dataset.getName(),
        snapshotId,
        refIds);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
