package bio.terra.service.snapshot.flight.create;

import bio.terra.common.DaoUtils;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.exception.SnapshotAlreadyExistsException;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import bio.terra.stairway.FlightUtils;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotSummary;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.dao.DataAccessException;
import org.springframework.http.HttpStatus;

import java.util.UUID;

public class CreateSnapshotMetadataStep implements Step {
    private SnapshotDao snapshotDao;
    private SnapshotService snapshotService;
    private SnapshotRequestModel snapshotReq;

    private static Logger logger = LoggerFactory.getLogger(CreateSnapshotMetadataStep.class);

    public CreateSnapshotMetadataStep(
        SnapshotDao snapshotDao, SnapshotService snapshotService, SnapshotRequestModel snapshotReq) {
        this.snapshotDao = snapshotDao;
        this.snapshotService = snapshotService;
        this.snapshotReq = snapshotReq;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        try {
            Snapshot snapshot = snapshotService.makeSnapshotFromSnapshotRequest(snapshotReq);
            UUID snapshotId = snapshotDao.create(snapshot);
            context.getWorkingMap().put(SnapshotWorkingMapKeys.SNAPSHOT_ID, snapshotId);
            SnapshotSummary snapshotSummary = snapshotDao.retrieveSnapshotSummary(snapshot.getId());
            SnapshotSummaryModel response = snapshotService.makeSummaryModelFromSummary(snapshotSummary);
            FlightUtils.setResponse(context, response, HttpStatus.CREATED);
            return StepResult.getStepResultSuccess();
        } catch (SnapshotAlreadyExistsException snapshotExistsEx) {
            // snapshot creation failed because of a PK violation
            // this happens when trying to create a snapshot with the same name as one that already exists
            // in this case, we don't want to delete the metadata in the undo step
            // so, set the SNAPSHOT_ID key in the context map to true, indicating to the undo step that the
            // snapshot already exists.
            context.getWorkingMap().put(JobMapKeys.SNAPSHOT_ID.getKeyName(), Boolean.TRUE);
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, snapshotExistsEx);
        } catch (SnapshotNotFoundException ex) {
            FlightUtils.setErrorResponse(context, ex.toString(), HttpStatus.BAD_REQUEST);
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
        }
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        // if this step failed because there is already a snapshot with this name, then don't delete the metadata
        Boolean snapshotIdExists = context.getWorkingMap().get(JobMapKeys.SNAPSHOT_ID.getKeyName(), Boolean.class);
        if (snapshotIdExists != null && snapshotIdExists.booleanValue()) {
            logger.debug("Snapshot creation failed because of a PK violation. Not deleting metadata.");
        } else {
            logger.debug("Snapshot creation failed for a reason other than a PK violation. Deleting metadata.");
            String snapshotName = snapshotReq.getName();
            snapshotDao.deleteByName(snapshotName);
        }
        return StepResult.getStepResultSuccess();
    }

}
