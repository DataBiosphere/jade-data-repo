package bio.terra.service.dataset.flight.create;

import bio.terra.common.exception.PdaoException;
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
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public class CreateDatasetAuthzGetCurrentPrimaryAclStep implements Step {
    private static final Logger logger = LoggerFactory.getLogger(CreateDatasetAuthzGetCurrentPrimaryAclStep.class);

    private final BigQueryPdao bigQueryPdao;
    private final DatasetService datasetService;

    public CreateDatasetAuthzGetCurrentPrimaryAclStep(
        BigQueryPdao bigQueryPdao,
        DatasetService datasetService) {
        this.bigQueryPdao = bigQueryPdao;
        this.datasetService = datasetService;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        FlightMap workingMap = context.getWorkingMap();
        UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);
        List<String> policyEmails = workingMap.get(DatasetWorkingMapKeys.POLICY_EMAILS, List.class);
        Dataset dataset = datasetService.retrieve(datasetId);
        try {
            bigQueryPdao.grantReadAccessToDataset(dataset, policyEmails);
        } catch (BigQueryException ex) {
            // TODO: OK, so how do I know it is an IAM error and not some other error?
            //  First, I just scan for IAM in the message text and I log the BigQueryError reason. Then I
            //  can come back to this code and be more explicit about the reason.
            BigQueryError bqError = ex.getError();
            if (bqError != null) {
                logger.info("BigQueryError: reason=" + bqError.getReason() + " message=" + bqError.getMessage());
            }
            if (StringUtils.contains(ex.getMessage(), "IAM")) {
                return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, ex);
            }
            throw new PdaoException("Caught BQ exception while granting read access to dataset", ex);
        }
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) throws InterruptedException {
        FlightMap workingMap = context.getWorkingMap();
        UUID datasetId = workingMap.get(DatasetWorkingMapKeys.DATASET_ID, UUID.class);

        return StepResult.getStepResultSuccess();
    }
}
