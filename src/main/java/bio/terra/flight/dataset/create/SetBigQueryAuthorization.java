package bio.terra.flight.dataset.create;

import bio.terra.pdao.bigquery.BigQueryContainerInfo;
import bio.terra.pdao.bigquery.BigQueryPdao;
import bio.terra.service.JobMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SetBigQueryAuthorization implements Step {
    private Logger logger = LoggerFactory.getLogger(SetBigQueryAuthorization.class);

    private BigQueryPdao bigQueryPdao;

    public SetBigQueryAuthorization(BigQueryPdao bigQueryPdao) {
        this.bigQueryPdao = bigQueryPdao;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        BigQueryContainerInfo bqInfo = workingMap.get(
            JobMapKeys.BQ_DATASET_INFO.getKeyName(), BigQueryContainerInfo.class);

        logger.debug(bqInfo.toString());
        bigQueryPdao.authorizeDatasetViewsForStudies(bqInfo);
        bigQueryPdao.addReaderGroupToDataset(bqInfo.getBqDatasetId(), bqInfo.getReadersEmail());

        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // if this step fails we only need to undo removing the authorized views on the study
        FlightMap workingMap = context.getWorkingMap();
        BigQueryContainerInfo bqInfo = workingMap.get(
            JobMapKeys.BQ_DATASET_INFO.getKeyName(), BigQueryContainerInfo.class);
        bigQueryPdao.removeDatasetAuthorizationFromStudies(bqInfo);
        return StepResult.getStepResultSuccess();
    }


}
