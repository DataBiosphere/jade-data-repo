package bio.terra.service.snapshot.flight.create;

import bio.terra.grammar.Query;
import bio.terra.grammar.google.BigQueryVisitor;
import bio.terra.model.DatasetModel;
import bio.terra.model.SnapshotRequestContentsModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestQueryModel;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.AssetTable;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
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
        // This assumption has already been validated as part of the flight
        /*
         * get dataset and assetName
         * get asset from dataset
         * which give the root table
         * then pass the row id array into create snapshot
         */
        SnapshotRequestContentsModel contentsModel = snapshotReq.getContents().get(0);
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
        AssetSpecification assetSpec = assetSpecOp.get();
        // TODO throw? assetSpecOp.isPresent() ? AssetNotFoundException
        AssetTable rootTable = assetSpec.getRootTable();

        Map<String, DatasetModel> datasetMap = Collections.singletonMap(datasetName, datasetModel);
        BigQueryVisitor bqVisitor = new BigQueryVisitor(datasetMap);

        String sqlQuery = query.translateSql(bqVisitor);

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

