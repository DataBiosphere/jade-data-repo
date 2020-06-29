package bio.terra.service.dataset.flight.create;

import bio.terra.common.FlightUtils;
import bio.terra.common.exception.PdaoException;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.flight.DatasetWorkingMapKeys;
import bio.terra.service.tabulardata.google.BigQueryPdao;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import com.google.cloud.bigquery.BigQueryError;
import com.google.cloud.bigquery.BigQueryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

import static bio.terra.service.configuration.ConfigEnum.DATASET_GRANT_ACCESS_FAULT;

public class CreateDatasetAuthzPrimaryDataStep implements Step {
    private static final Logger logger = LoggerFactory.getLogger(CreateDatasetAuthzPrimaryDataStep.class);

    private final BigQueryPdao bigQueryPdao;
    private final DatasetService datasetService;
    private final ConfigurationService configService;

    public CreateDatasetAuthzPrimaryDataStep(
        BigQueryPdao bigQueryPdao,
        DatasetService datasetService,
        ConfigurationService configService) {
        this.bigQueryPdao = bigQueryPdao;
        this.datasetService = datasetService;
        this.configService = configService;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        FlightMap workingMap = context.getWorkingMap();
        UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
        List<String> policyEmails = workingMap.get(DatasetWorkingMapKeys.POLICY_EMAILS, List.class);
        Dataset dataset = datasetService.retrieve(datasetId);
        try {
            if (configService.testInsertFault(DATASET_GRANT_ACCESS_FAULT)) {
                throw new BigQueryException(400, "IAM setPolicy fake failure",
                    new BigQueryError("invalid", "fake", "IAM setPolicy fake failure"));
            }

            bigQueryPdao.grantReadAccessToDataset(dataset, policyEmails);
        } catch (BigQueryException ex) {
            if (FlightUtils.isBigQueryIamPropagationError(ex)) {
                return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, ex);
            }
            throw new PdaoException("Caught BQ exception while granting read access to dataset", ex);
        }
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) throws InterruptedException {
        // We do not try to undo the ACL set, because we expect the entire dataset create to be undone.
        return StepResult.getStepResultSuccess();
    }
}
