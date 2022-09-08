package bio.terra.service.snapshot.flight.export;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.AccessInfoParquetModelTable;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRetrieveIncludeModel;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public class SnapshotExportListAzureParquetFilesStep extends DefaultUndoStep {

  private final SnapshotService snapshotService;
  private final UUID snapshotId;
  private final AzureBlobStorePdao azureBlobStorePdao;
  private final AuthenticatedUserRequest userReq;

  public SnapshotExportListAzureParquetFilesStep(
      SnapshotService snapshotService,
      UUID snapshotId,
      AzureBlobStorePdao azureBlobStorePdao,
      AuthenticatedUserRequest userReq) {
    this.snapshotService = snapshotService;
    this.snapshotId = snapshotId;
    this.azureBlobStorePdao = azureBlobStorePdao;
    this.userReq = userReq;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    FlightMap workingMap = context.getWorkingMap();
    SnapshotModel snapshot =
        snapshotService.retrieveAvailableSnapshotModel(
            snapshotId,
            List.of(
                SnapshotRetrieveIncludeModel.ACCESS_INFORMATION,
                SnapshotRetrieveIncludeModel.PROFILE),
            userReq);

    Map<String, List<String>> tablesToPaths =
        snapshot.getAccessInformation().getParquet().getTables().stream()
            .collect(
                Collectors.toMap(
                    AccessInfoParquetModelTable::getName,
                    t ->
                        azureBlobStorePdao.listChildren(
                            "%s?%s".formatted(t.getUrl(), t.getSasToken()))));

    workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_PARQUET_PATHS, tablesToPaths);
    workingMap.put(JobMapKeys.BILLING_ID.getKeyName(), snapshot.getProfileId());
    return StepResult.getStepResultSuccess();
  }
}
