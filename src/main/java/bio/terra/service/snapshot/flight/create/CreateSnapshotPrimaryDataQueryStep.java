package bio.terra.service.snapshot.flight.create;

import bio.terra.grammar.Query;
import bio.terra.grammar.exception.InvalidQueryException;
import bio.terra.grammar.google.BigQueryVisitor;
import bio.terra.model.DatasetModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestQueryModel;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.exception.AssetNotFoundException;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public class CreateSnapshotPrimaryDataQueryStep implements Step {

    private BigQueryPdao bigQueryPdao;
    private DatasetService datasetService;
    private SnapshotDao snapshotDao;
    private SnapshotRequestModel snapshotReq;

    public CreateSnapshotPrimaryDataQueryStep(BigQueryPdao bigQueryPdao,
                                              DatasetService datasetService,
                                              SnapshotDao snapshotDao,
                                              SnapshotRequestModel snapshotReq) {
        this.bigQueryPdao = bigQueryPdao;
        this.datasetService = datasetService;
        this.snapshotDao = snapshotDao;
        this.snapshotReq = snapshotReq;
    }

    @Override
    public StepResult doStep(FlightContext context) {
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
        DatasetModel datasetModel = datasetService.retrieveModel(dataset.getId());

        // get asset out of dataset
        Optional<AssetSpecification> assetSpecOp = dataset.getAssetSpecificationByName(snapshotAssetName);
        AssetSpecification assetSpec = assetSpecOp.orElseThrow(
            () -> new AssetNotFoundException("Expected asset specification"));

        Map<String, DatasetModel> datasetMap = Collections.singletonMap(datasetName, datasetModel);
        BigQueryVisitor bqVisitor = new BigQueryVisitor(datasetMap);

        String sqlQuery = query.translateSql(bqVisitor);

        // validate that the root table is actually a table being queried in the query -->
        // TODO specifically validate that it's in the FROM clause
        List<String> tableNames = query.getTableNames();
        String rootTablename = assetSpec.getRootTable().getTable().getName();
        if (!tableNames.contains(rootTablename)) {
            throw new InvalidQueryException("The root table of the selected asset is not present in this query");
        }

        // now using the query, get the rowIds
        // insert the rowIds into the snapshot row ids table and then kick off the rest of the relationship walking
        bigQueryPdao.queryForRowIds(assetSpec, snapshot, sqlQuery);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // Remove any file dependencies created
        Snapshot snapshot = snapshotDao.retrieveSnapshotByName(snapshotReq.getName());
        bigQueryPdao.deleteSnapshot(snapshot);
        return StepResult.getStepResultSuccess();
    }

}

