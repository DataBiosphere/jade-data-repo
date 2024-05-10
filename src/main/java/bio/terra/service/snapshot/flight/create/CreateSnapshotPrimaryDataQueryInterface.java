package bio.terra.service.snapshot.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.grammar.Query;
import bio.terra.grammar.exception.InvalidQueryException;
import bio.terra.model.SnapshotAccessRequestResponse;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestQueryModel;
import bio.terra.service.common.CommonFlightUtils;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.exception.AssetNotFoundException;
import bio.terra.service.snapshotbuilder.SnapshotBuilderService;
import bio.terra.service.snapshotbuilder.SnapshotRequestDao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.StepResult;
import java.time.Instant;
import java.util.List;
import java.util.UUID;

public interface CreateSnapshotPrimaryDataQueryInterface {

  default StepResult prepareQueryAndCreateSnapshot(
      FlightContext context,
      Snapshot snapshot,
      SnapshotRequestModel snapshotReq,
      DatasetService datasetService,
      SnapshotBuilderService snapshotBuilderService,
      SnapshotRequestDao snapshotRequestDao,
      SnapshotDao snapshotDao,
      AuthenticatedUserRequest userReq)
      throws InterruptedException {
    AssetSpecification assetSpecification;
    String sqlQuery;
    Instant createdAt;

    if (snapshotReq
        .getContents()
        .get(0)
        .getMode()
        .equals(SnapshotRequestContentsModel.ModeEnum.BYREQUESTID)) {
      UUID accessRequestId =
          snapshotReq.getContents().get(0).getRequestIdSpec().getSnapshotRequestId();
      SnapshotAccessRequestResponse accessRequest = snapshotRequestDao.getById(accessRequestId);

      UUID dataReleaseSnapshotId = accessRequest.getSourceSnapshotId();
      Snapshot dataReleaseSnapshot = snapshotDao.retrieveSnapshot(dataReleaseSnapshotId);
      // get the underlying dataset for the snapshot
      if (dataReleaseSnapshot.getSnapshotSources().isEmpty()) {
        throw new IllegalArgumentException("Snapshot does not have a source dataset");
      }
      Dataset dataset = dataReleaseSnapshot.getSnapshotSources().get(0).getDataset();
      // gets pre-existing asset on the dataset
      // TODO: create custom asset
      assetSpecification = dataset.getAssetSpecificationByName("person_visit").orElseThrow();
      sqlQuery =
          snapshotBuilderService.generateRowIdQuery(accessRequest, dataReleaseSnapshot, userReq);
      createdAt = dataReleaseSnapshot.getCreatedDate();
    } else {
      SnapshotRequestQueryModel snapshotQuerySpec = snapshotReq.getContents().get(0).getQuerySpec();

      Query query = Query.parse(snapshotQuerySpec.getQuery());
      String datasetName = query.getDatasetName();
      Dataset dataset = datasetService.retrieveByName(datasetName);
      assetSpecification = retrieveAssetSpecification(dataset, snapshotQuerySpec.getAssetName());
      validateRootTable(query, assetSpecification);
      sqlQuery = translateQuery(query, dataset);
      createdAt = CommonFlightUtils.getCreatedAt(context);
    }

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
