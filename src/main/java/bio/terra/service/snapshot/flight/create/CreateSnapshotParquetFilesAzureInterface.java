package bio.terra.service.snapshot.flight.create;

import static bio.terra.common.PdaoConstant.PDAO_ROW_ID_TABLE;

import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotTable;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

public interface CreateSnapshotParquetFilesAzureInterface {

  default void createRowIdsAndStoreRowCount(
      FlightContext context,
      AzureSynapsePdao azureSynapsePdao,
      SnapshotService snapshotService,
      Map<String, Long> tableRowCounts)
      throws SQLException {
    FlightMap workingMap = context.getWorkingMap();
    UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);

    List<SnapshotTable> tables = snapshotService.retrieveTables(snapshotId);
    azureSynapsePdao.createSnapshotRowIdsParquetFile(
        tables,
        snapshotId,
        IngestUtils.getTargetDataSourceName(context.getFlightId()),
        tableRowCounts);

    workingMap.put(SnapshotWorkingMapKeys.TABLE_ROW_COUNT_MAP, tableRowCounts);
  }

  default void undoCreateSnapshotParquetFiles(
      FlightContext context, SnapshotService snapshotService, AzureSynapsePdao azureSynapsePdao) {
    FlightMap workingMap = context.getWorkingMap();
    UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);
    List<SnapshotTable> tables = snapshotService.retrieveTables(snapshotId);

    azureSynapsePdao.dropTables(
        tables.stream()
            .map(table -> IngestUtils.formatSnapshotTableName(snapshotId, table.getName()))
            .collect(Collectors.toList()));
    azureSynapsePdao.dropTables(
        List.of(IngestUtils.formatSnapshotTableName(snapshotId, PDAO_ROW_ID_TABLE)));
  }
}
