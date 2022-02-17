package bio.terra.service.snapshot.flight.export;

import bio.terra.service.common.gcs.BigQueryUtils;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.RetryException;
import java.util.UUID;

public class SnapshotExportWriteMappingTable implements Step {

  private final UUID snapshotId;
  private final SnapshotService snapshotService;
  private final BigQueryPdao bigQueryPdao;

  public SnapshotExportWriteMappingTable(
      UUID snapshotId, SnapshotService snapshotService, BigQueryPdao bigQueryPdao) {
    this.snapshotId = snapshotId;
    this.snapshotService = snapshotService;
    this.bigQueryPdao = bigQueryPdao;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException, RetryException {
    Snapshot snapshot = snapshotService.retrieve(snapshotId);
    FlightMap workingMap = context.getWorkingMap();
    String gsPathMappingFile =
        workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_EXPORT_GSPATHS_FILENAME, String.class);
    String tableName = BigQueryUtils.firestoreDumpTableName(snapshot);
    String suffix = BigQueryUtils.getSuffix(context);
    bigQueryPdao.createFirestoreGsPathExternalTable(snapshot, gsPathMappingFile, tableName, suffix);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    Snapshot snapshot = snapshotService.retrieve(snapshotId);
    String tableName = BigQueryUtils.firestoreDumpTableName(snapshot);
    String suffix = BigQueryUtils.getSuffix(context);
    bigQueryPdao.deleteExternalTable(snapshot, tableName, suffix);
    return StepResult.getStepResultSuccess();
  }
}
