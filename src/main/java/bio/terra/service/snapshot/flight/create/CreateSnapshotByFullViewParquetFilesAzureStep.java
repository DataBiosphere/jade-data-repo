package bio.terra.service.snapshot.flight.create;

import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class CreateSnapshotByFullViewParquetFilesAzureStep
    implements Step, CreateSnapshotParquetFilesAzureInterface {

  protected AzureSynapsePdao azureSynapsePdao;
  private final SnapshotService snapshotService;
  protected final SnapshotRequestModel snapshotReq;

  public CreateSnapshotByFullViewParquetFilesAzureStep(
      AzureSynapsePdao azureSynapsePdao,
      SnapshotService snapshotService,
      SnapshotRequestModel snapshotReq) {
    this.azureSynapsePdao = azureSynapsePdao;
    this.snapshotService = snapshotService;
    this.snapshotReq = snapshotReq;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    return createSnapshotParquetFiles(context, azureSynapsePdao, snapshotService);
  }

  @Override
  public Map<String, Long> createSnapshotPrimaryDataParquetFiles(FlightContext context)
      throws SQLException {
    FlightMap workingMap = context.getWorkingMap();
    UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);

    List<SnapshotTable> tables = snapshotService.retrieveTables(snapshotId);
    return azureSynapsePdao.createSnapshotParquetFiles(
        tables,
        snapshotId,
        IngestUtils.getSourceDatasetDataSourceName(context.getFlightId()),
        IngestUtils.getTargetDataSourceName(context.getFlightId()),
        null,
        snapshotReq.isGlobalFileIds());
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    undoCreateSnapshotParquetFiles(context, snapshotService, azureSynapsePdao);
    return StepResult.getStepResultSuccess();
  }
}
