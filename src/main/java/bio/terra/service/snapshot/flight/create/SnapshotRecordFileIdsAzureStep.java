package bio.terra.service.snapshot.flight.create;

import bio.terra.common.FlightUtils;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.DrsIdService;
import bio.terra.service.filedata.DrsService;
import bio.terra.service.filedata.azure.tables.TableDependencyDao;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import com.azure.data.tables.TableServiceClient;
import java.util.List;
import java.util.UUID;

public class SnapshotRecordFileIdsAzureStep extends SnapshotRecordFileIdsStep {

  private final TableDependencyDao tableDependencyDao;
  private final AzureAuthService azureAuthService;

  public SnapshotRecordFileIdsAzureStep(
      SnapshotService snapshotService,
      DatasetService datasetService,
      DrsIdService drsIdService,
      DrsService drsService,
      TableDependencyDao tableDependencyDao,
      AzureAuthService azureAuthService) {
    super(snapshotService, datasetService, drsIdService, drsService);
    this.tableDependencyDao = tableDependencyDao;
    this.azureAuthService = azureAuthService;
  }

  @Override
  List<String> getFileIds(FlightContext context, Dataset dataset, UUID snapshotId)
      throws InterruptedException {
    AzureStorageAuthInfo datasetStorageAuthInfo =
        FlightUtils.getContextValue(
            context, CommonMapKeys.DATASET_STORAGE_AUTH_INFO, AzureStorageAuthInfo.class);
    TableServiceClient datasetTableServiceClient =
        azureAuthService.getTableServiceClient(datasetStorageAuthInfo);

    return tableDependencyDao.getDatasetSnapshotFileIds(
        datasetTableServiceClient, dataset, snapshotId.toString());
  }
}
