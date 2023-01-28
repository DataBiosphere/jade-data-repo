package bio.terra.service.snapshot.flight.create;

import bio.terra.grammar.Query;
import bio.terra.grammar.exception.InvalidQueryException;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestQueryModel;
import bio.terra.service.common.CommonFlightUtils;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.exception.AssetNotFoundException;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.time.Instant;
import java.util.List;

public interface CreateSnapshotPrimaryDataQueryInterface {

  default StepResult prepareQueryAndCreateSnapshot(
      FlightContext context,
      Snapshot snapshot,
      SnapshotRequestModel snapshotReq,
      DatasetService datasetService)
      throws InterruptedException {
    SnapshotRequestQueryModel snapshotQuerySpec = snapshotReq.getContents().get(0).getQuerySpec();

    Query query = Query.parse(snapshotQuerySpec.getQuery());
    String datasetName = query.getDatasetName();
    Dataset dataset = datasetService.retrieveByName(datasetName);
    AssetSpecification assetSpecification =
        retrieveAssetSpecification(dataset, snapshotQuerySpec.getAssetName());
    validateRootTable(query, assetSpecification);
    String sqlQuery = translateQuery(query, dataset);

    Instant createdAt = CommonFlightUtils.getCreatedAt(context);
    return createSnapshotPrimaryData(context, assetSpecification, snapshot, sqlQuery, createdAt);
  }

  StepResult createSnapshotPrimaryData(
      FlightContext context,
      AssetSpecification assetSpecification,
      Snapshot snapshot,
      String sqlQuery,
      Instant filterBefore)
      throws InterruptedException;

  String translateQuery(Query query, Dataset dataset);

  default AssetSpecification retrieveAssetSpecification(Dataset dataset, String assetName) {
    return dataset
        .getAssetSpecificationByName(assetName)
        .orElseThrow(() -> new AssetNotFoundException("Expected asset specification"));
  }

  /**
   * validate that the root table is actually a table being queried in the query --> and the grammar
   * only picks up tables names in the from clause (though there may be more than one)
   */
  default void validateRootTable(Query query, AssetSpecification assetSpecification) {
    List<String> tableNames = query.getTableNames();
    String rootTableName = assetSpecification.getRootTable().getTable().getName();
    if (!tableNames.contains(rootTableName)) {
      throw new InvalidQueryException(
          "The root table of the selected asset is not present in this query");
    }
  }
}
