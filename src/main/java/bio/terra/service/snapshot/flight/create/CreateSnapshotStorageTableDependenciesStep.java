package bio.terra.service.snapshot.flight.create;

import bio.terra.common.FlightUtils;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.filedata.azure.tables.TableDependencyDao;
import bio.terra.service.filedata.flight.FileMapKeys;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.azure.data.tables.TableServiceClient;

public class CreateSnapshotStorageTableDependenciesStep implements Step {
  private final TableDao tableDao;
  private final TableDependencyDao tableDependencyDao;
  private final AzureAuthService azureAuthService;

  public CreateSnapshotStorageTableDependenciesStep(TableDao tableDao,
                                                    TableDependencyDao tableDependencyDao,
                                                    AzureAuthService azureAuthService) {
    this.tableDao = tableDao;
    this.tableDependencyDao = tableDependencyDao;
    this.azureAuthService = azureAuthService;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
     FlightMap workingMap = context.getWorkingMap();
     AzureStorageAuthInfo storageAuthInfo = FlightUtils.getContextValue(
                context, FileMapKeys.STORAGE_AUTH_INFO, AzureStorageAuthInfo.class);
    TableServiceClient datasetTableServiceClient = azureAuthService.getTableServiceClient(storageAuthInfo);

    tableDependencyDao.storeSnapshotFileDependencies(
        datasetTableServiceClient, DATASET_ID, SNAPSHOT_ID2, REF_IDS);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {

    return StepResult.getStepResultSuccess();
  }
}
