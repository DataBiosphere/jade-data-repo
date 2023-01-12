package bio.terra.service.snapshot.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.grammar.Query;
import bio.terra.grammar.google.BigQueryVisitor;
import bio.terra.model.DatasetModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestQueryModel;
import bio.terra.service.common.CommonFlightUtils;
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
import java.util.Collections;
import java.util.Map;

public class CreateSnapshotPrimaryDataQueryGcpStep implements Step {
  private final BigQuerySnapshotPdao bigQuerySnapshotPdao;
  private final SnapshotService snapshotService;
  private final DatasetService datasetService;
  private final SnapshotDao snapshotDao;
  private final SnapshotRequestModel snapshotReq;
  private final AuthenticatedUserRequest userRequest;
  private final CreateSnapshotPrimaryDataQueryUtils createSnapshotPrimaryDataQueryUtils;

  public CreateSnapshotPrimaryDataQueryGcpStep(
      BigQuerySnapshotPdao bigQuerySnapshotPdao,
      SnapshotService snapshotService,
      DatasetService datasetService,
      SnapshotDao snapshotDao,
      SnapshotRequestModel snapshotReq,
      AuthenticatedUserRequest userRequest,
      CreateSnapshotPrimaryDataQueryUtils createSnapshotPrimaryDataQueryUtils) {
    this.bigQuerySnapshotPdao = bigQuerySnapshotPdao;
    this.snapshotService = snapshotService;
    this.datasetService = datasetService;
    this.snapshotDao = snapshotDao;
    this.snapshotReq = snapshotReq;
    this.userRequest = userRequest;
    this.createSnapshotPrimaryDataQueryUtils = createSnapshotPrimaryDataQueryUtils;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
    SnapshotRequestQueryModel snapshotQuerySpec = snapshotReq.getContents().get(0).getQuerySpec();

    Query query = Query.parse(snapshotQuerySpec.getQuery());
    Dataset dataset = createSnapshotPrimaryDataQueryUtils.retrieveDatasetSpecifiedByQuery(query);
    AssetSpecification assetSpecification =
        createSnapshotPrimaryDataQueryUtils.retrieveAssetSpecification(
            dataset, snapshotQuerySpec.getAssetName());
    createSnapshotPrimaryDataQueryUtils.validateRootTable(query, assetSpecification);
    String sqlQuery = translateBQQuery(query, dataset);

    Instant createdAt = CommonFlightUtils.getCreatedAt(context);

    bigQuerySnapshotPdao.queryForRowIds(assetSpecification, snapshot, sqlQuery, createdAt);
    return StepResult.getStepResultSuccess();
  }

  /**
   * Translate user query to be used in BigQuery
   *
   * @param query
   * @param dataset
   * @return
   */
  private String translateBQQuery(Query query, Dataset dataset) {
    DatasetModel datasetModel = datasetService.retrieveModel(dataset, userRequest);
    Map<String, DatasetModel> datasetMap =
        Collections.singletonMap(dataset.getName(), datasetModel);
    BigQueryVisitor bqVisitor = new BigQueryVisitor(datasetMap);
    return query.translateSql(bqVisitor);
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    snapshotService.undoCreateSnapshot(snapshotReq.getName());
    return StepResult.getStepResultSuccess();
  }
}
