package bio.terra.service.snapshot.flight.create;

import bio.terra.model.SnapshotRequestAssetModel;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestRowIdModel;
import bio.terra.service.common.CommonFlightUtils;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotSource;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.stairway.FlightContext;

import java.sql.SQLException;
import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class CreateSnapshotByAssetParquetFilesAzureStep
    extends CreateSnapshotParquetFilesAzureStep {
  private final SnapshotRequestModel snapshotReq;
  private final SnapshotDao snapshotDao;

  public CreateSnapshotByAssetParquetFilesAzureStep(
      AzureSynapsePdao azureSynapsePdao,
      SnapshotDao snapshotDao,
      SnapshotService snapshotService,
      SnapshotRequestModel snapshotReq) {
    super(azureSynapsePdao, snapshotService);
    this.snapshotReq = snapshotReq;
    this.snapshotDao = snapshotDao;
  }

  @Override
  public Map<String, Long> createSnapshotParquetFiles(
      List<SnapshotTable> tables, UUID snapshotId, FlightContext context) throws SQLException {
    SnapshotRequestContentsModel contentsModel = snapshotReq.getContents().get(0);
    SnapshotRequestAssetModel assetModel = contentsModel.getAssetSpec();

    Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
    SnapshotSource source = snapshot.getFirstSnapshotSource();

    AssetSpecification assetSpec = source.getAssetSpecification();

    return azureSynapsePdao.createSnapshotParquetFilesByAsset(
        tables,
        snapshotId,
        IngestUtils.getSourceDatasetDataSourceName(context.getFlightId()),
        IngestUtils.getTargetDataSourceName(context.getFlightId()),
        null,
        rowIdModel);
  }
}
