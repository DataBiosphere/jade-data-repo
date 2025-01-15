package bio.terra.service.snapshot.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.grammar.Query;
import bio.terra.grammar.google.BigQueryVisitor;
import bio.terra.model.DatasetModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.time.Instant;
import java.util.Map;

public record CreateSnapshotPrimaryDataQueryGcpStep(
    BigQuerySnapshotPdao bigQuerySnapshotPdao,
    SnapshotService snapshotService,
    DatasetService datasetService,
    SnapshotDao snapshotDao,
    SnapshotRequestModel snapshotReq,
    AuthenticatedUserRequest userRequest,
    Dataset sourceDataset)
    implements CreateSnapshotPrimaryDataQueryInterface, Step {

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
    return prepareQueryAndCreateSnapshot(context, snapshot, snapshotReq, sourceDataset);
  }

  @Override
  public StepResult createSnapshotPrimaryData(
      FlightContext context,
      AssetSpecification assetSpecification,
      Snapshot snapshot,
      String sqlQuery,
      Instant filterBefore)
      throws InterruptedException {
    bigQuerySnapshotPdao.createSnapshotByQuery(
        assetSpecification, snapshot, sqlQuery, filterBefore);
    return StepResult.getStepResultSuccess();
  }

  /**
   * Translate user query to be used in BigQuery
   *
   * @param query
   * @param dataset
   * @return
   */
  @Override
  public String translateQuery(Query query, Dataset dataset) {
    DatasetModel datasetModel = datasetService.retrieveModel(dataset, userRequest);
    BigQueryVisitor bqVisitor = new BigQueryVisitor(Map.of(dataset.getName(), datasetModel));
    return query.translateSql(bqVisitor);
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    snapshotService.undoCreateSnapshot(snapshotReq.getName());
    return StepResult.getStepResultSuccess();
  }
}
