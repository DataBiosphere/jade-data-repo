package bio.terra.service.snapshot.flight.create;

import bio.terra.common.FlightUtils;
import bio.terra.service.common.CommonMapKeys;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.DrsIdService;
import bio.terra.service.filedata.DrsService;
import bio.terra.service.filedata.azure.tables.TableDao;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAuthInfo;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import com.azure.data.tables.TableServiceClient;
import java.util.List;
import java.util.UUID;

public class SnapshotRecordFileIdsAzureStep extends SnapshotRecordFileIdsStep {

  private final TableDao tableDao;
  private final AzureAuthService azureAuthService;

  public SnapshotRecordFileIdsAzureStep(
      SnapshotService snapshotService,
      DatasetService datasetService,
      DrsIdService drsIdService,
      DrsService drsService,
      TableDao tableDao,
      AzureAuthService azureAuthService,
      UUID snapshotId) {
    super(snapshotService, datasetService, drsIdService, drsService, snapshotId);
    this.tableDao = tableDao;
    this.azureAuthService = azureAuthService;
  }

  @Override
  List<String> getFileIds(FlightContext context, Snapshot snapshot) throws InterruptedException {
    AzureStorageAuthInfo snapshotStorageAuthInfo =
        FlightUtils.getContextValue(
            context, CommonMapKeys.SNAPSHOT_STORAGE_AUTH_INFO, AzureStorageAuthInfo.class);
    TableServiceClient snapshotTableServiceClient =
        azureAuthService.getTableServiceClient(snapshotStorageAuthInfo);

    return tableDao.retrieveAllFileIds(snapshotTableServiceClient, snapshot.getId());
  }
}
