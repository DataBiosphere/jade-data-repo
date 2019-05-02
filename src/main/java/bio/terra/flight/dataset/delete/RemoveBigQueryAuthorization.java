package bio.terra.flight.dataset.delete;

import bio.terra.dao.DatasetDao;
import bio.terra.metadata.Dataset;
import bio.terra.metadata.DatasetSource;
import bio.terra.pdao.bigquery.BigQueryContainerInfo;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public class RemoveBigQueryAuthorization implements Step {
    private Logger logger = LoggerFactory.getLogger(RemoveBigQueryAuthorization.class);

    private BigQueryPdao bigQueryPdao;
    private DatasetDao datasetDao;
    private UUID datasetId;

    public RemoveBigQueryAuthorization(BigQueryPdao bigQueryPdao, DatasetDao datasetDao, UUID datasetId) {
        this.bigQueryPdao = bigQueryPdao;
        this.datasetDao = datasetDao;
        this.datasetId = datasetId;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        Dataset ds = datasetDao.retrieveDataset(datasetId);
        List<DatasetSource> sources = ds.getDatasetSources();

        BigQueryContainerInfo bqInfo = new BigQueryContainerInfo().bqDatasetId(ds.getName());
        sources.stream().forEach(dsSource ->
            bqInfo.addTables(bigQueryPdao.prefixName(dsSource.getStudy().getName()),
                dsSource.getAssetSpecification().getAssetTables().stream().map(assetTable ->
                    assetTable.getTable().getName()).collect(Collectors.toList())));
        logger.debug(bqInfo.toString());
        bigQueryPdao.removeDatasetAuthorizationFromStudies(bqInfo);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // can't undo this step
        return StepResult.getStepResultSuccess();
    }
}
