package bio.terra.service.snapshot.flight.create;

import bio.terra.common.FlightUtils;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.azure.data.tables.TableServiceClient;

public class CreateSnapshotStorageTableDataStep implements Step {
  private final TableDao tableDao;
  private final AzureAuthService azureAuthService;
  private final DatasetService datasetService;


  public CreateSnapshotStorageTableDataStep(TableDao tableDao,
                                            AzureAuthService azureAuthService,
                                            DatasetService datasetService) {
    this.tableDao = tableDao;
    this.azureAuthService = azureAuthService;
    this.datasetService = datasetService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    AzureStorageAuthInfo storageAuthInfo =
            FlightUtils.getContextValue(
                context, FileMapKeys.STORAGE_AUTH_INFO, AzureStorageAuthInfo.class);
   TableServiceClient tableServiceClient = azureAuthService.getTableServiceClient(storageAuthInfo);

    Dataset dataset = IngestUtils.getDataset(context, datasetService);

    // TODO - add reference
    tableDao.addFilesToSnapshot(tableServiceClient, tableServiceClient, dataset.getId(), dataset.getName(), snapshot, refIds);


    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
