package bio.terra.service.snapshot.flight.create;

import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestRowIdModel;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.stairway.FlightContext;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class CreateSnapshotByRowIdParquetFilesAzureStep
    extends CreateSnapshotParquetFilesAzureStep {

  public CreateSnapshotByRowIdParquetFilesAzureStep(
      AzureSynapsePdao azureSynapsePdao,
      SnapshotService snapshotService,
      SnapshotRequestModel snapshotReq) {
    super(azureSynapsePdao, snapshotService, snapshotReq);
  }

  @Override
  public Map<String, Long> createSnapshotParquetFiles(
      List<SnapshotTable> tables, UUID snapshotId, FlightContext context) throws SQLException {
    SnapshotRequestContentsModel contentsModel = snapshotReq.getContents().get(0);
    SnapshotRequestRowIdModel rowIdModel = contentsModel.getRowIdSpec();

    return azureSynapsePdao.createSnapshotParquetFilesByRowId(
        tables,
        snapshotId,
        IngestUtils.getSourceDatasetDataSourceName(context.getFlightId()),
        IngestUtils.getTargetDataSourceName(context.getFlightId()),
        null,
        rowIdModel,
        snapshotReq.isGlobalFileIds());
  }
}
