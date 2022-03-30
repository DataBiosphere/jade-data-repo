package bio.terra.service.snapshot.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.grammar.Query;
import bio.terra.grammar.exception.InvalidQueryException;
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
import bio.terra.service.snapshot.exception.AssetNotFoundException;
import bio.terra.service.tabulardata.google.bigquery.BigQuerySnapshotPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CreateSnapshotPrimaryDataQueryStep implements Step {

  private final BigQuerySnapshotPdao bigQuerySnapshotPdao;
  private final DatasetService datasetService;
  private final SnapshotService snapshotService;
  private final SnapshotDao snapshotDao;
  private final SnapshotRequestModel snapshotReq;
  private final AuthenticatedUserRequest userRequest;

  public CreateSnapshotPrimaryDataQueryStep(
      BigQuerySnapshotPdao bigQuerySnapshotPdao,
      DatasetService datasetService,
      SnapshotService snapshotService,
      SnapshotDao snapshotDao,
      SnapshotRequestModel snapshotReq,
      AuthenticatedUserRequest userRequest) {
    this.bigQuerySnapshotPdao = bigQuerySnapshotPdao;
    this.datasetService = datasetService;
    this.snapshotService = snapshotService;
    this.snapshotDao = snapshotDao;
    this.snapshotReq = snapshotReq;
    this.userRequest = userRequest;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    // TODO: this assumes single-dataset snapshots, will need to add a loop for multiple
    // (based on the validation flight step that already occurred.)
    /*
     * get dataset and assetName
     * get asset from dataset
     * which gives the root table
     * to use in conjunction with the filtered row ids to create this snapshot
     */
    Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
    SnapshotRequestQueryModel snapshotQuerySpec = snapshotReq.getContents().get(0).getQuerySpec();
    String snapshotAssetName = snapshotQuerySpec.getAssetName();

    String snapshotQuery = snapshotReq.getContents().get(0).getQuerySpec().getQuery();
    Query query = Query.parse(snapshotQuery);
    List<String> datasetNames = query.getDatasetNames();
    // TODO this makes the assumption that there is only one dataset
    // (based on the validation flight step that already occurred.)
    // This will change when more than 1 dataset is allowed
    String datasetName = datasetNames.get(0);

    Dataset dataset = datasetService.retrieveByName(datasetName);
    DatasetModel datasetModel = datasetService.retrieveModel(dataset, userRequest);

    // get asset out of dataset
    Optional<AssetSpecification> assetSpecOp =
        dataset.getAssetSpecificationByName(snapshotAssetName);
    AssetSpecification assetSpec =
        assetSpecOp.orElseThrow(() -> new AssetNotFoundException("Expected asset specification"));

    Map<String, DatasetModel> datasetMap = Collections.singletonMap(datasetName, datasetModel);
    BigQueryVisitor bqVisitor = new BigQueryVisitor(datasetMap);

    String sqlQuery = query.translateSql(bqVisitor);

    // validate that the root table is actually a table being queried in the query -->
    // and the grammar only picks up tables names in the from clause (though there may be more than
    // one)
    List<String> tableNames = query.getTableNames();
    String rootTablename = assetSpec.getRootTable().getTable().getName();
    if (!tableNames.contains(rootTablename)) {
      throw new InvalidQueryException(
          "The root table of the selected asset is not present in this query");
    }

    // now using the query, get the rowIds
    // insert the rowIds into the snapshot row ids table and then kick off the rest of the
    // relationship walking
    Instant createdAt = CommonFlightUtils.getCreatedAt(context);

    bigQuerySnapshotPdao.queryForRowIds(assetSpec, snapshot, sqlQuery, createdAt);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    snapshotService.undoCreateSnapshot(snapshotReq.getName());
    return StepResult.getStepResultSuccess();
  }
}
