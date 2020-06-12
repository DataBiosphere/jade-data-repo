package bio.terra.service.snapshot.flight.create;

import bio.terra.common.exception.PdaoException;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
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

import java.util.UUID;

import static bio.terra.service.configuration.ConfigEnum.SNAPSHOT_GRANT_ACCESS_FAULT;

public class SnapshotAuthzTabularAclStep implements Step {
    private final BigQueryPdao bigQueryPdao;
    private final SnapshotService snapshotService;
    private final ConfigurationService configService;
    private static final Logger logger = LoggerFactory.getLogger(SnapshotAuthzTabularAclStep.class);

    public SnapshotAuthzTabularAclStep(BigQueryPdao bigQueryPdao,
                                       SnapshotService snapshotService,
                                       ConfigurationService configService) {
        this.bigQueryPdao = bigQueryPdao;
        this.snapshotService = snapshotService;
        this.configService = configService;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        FlightMap workingMap = context.getWorkingMap();
        UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);
        Snapshot snapshot = snapshotService.retrieve(snapshotId);

        String readersPolicyEmail = workingMap.get(SnapshotWorkingMapKeys.POLICY_EMAIL, String.class);

        try {
            if (configService.testInsertFault(SNAPSHOT_GRANT_ACCESS_FAULT)) {
                throw new BigQueryException(400, "Fake IAM failure", new BigQueryError("reasonTBD", "fake", "fake"));
            }

            bigQueryPdao.addReaderGroupToSnapshot(snapshot, readersPolicyEmail);
        } catch (BigQueryException ex) {
            // TODO: OK, so how do I know it is an IAM error and not some other error?
            //  First, I just scan for IAM in the message text and I log the BigQueryError reason. Then I
            //  can come back to this code and be more explicit about the reason.
            //  This is a cut'n'paste from CreateDatasetAuthzPrimaryDataStep. When we fix it there,
            //  we should also fix it here.
            BigQueryError bqError = ex.getError();
            if (bqError != null) {
                logger.info("BigQueryError: reason=" + bqError.getReason() + " message=" + bqError.getMessage());
            }
            if (StringUtils.contains(ex.getMessage(), "IAM")) {
                return new StepResult(StepStatus.STEP_RESULT_FAILURE_RETRY, ex);
            }
            throw new PdaoException("Caught BQ exception while granting read access to snapshot", ex);
        }
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // We do not try to undo the ACL set, because we expect the entire BQ dataset to get deleted,
        // taking the ACLs with it if we are on this undo path.
        return StepResult.getStepResultSuccess();
    }
}
