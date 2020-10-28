package bio.terra.service.snapshot.flight.create;

import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotSummaryModel;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDao;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.SnapshotSummary;
import bio.terra.service.snapshot.exception.InvalidSnapshotException;
import bio.terra.service.snapshot.exception.SnapshotNotFoundException;
import bio.terra.common.FlightUtils;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.StepStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;

import java.util.UUID;

public class CreateSnapshotMetadataStep implements Step {
    private final SnapshotDao snapshotDao;
    private final SnapshotService snapshotService;
    private final SnapshotRequestModel snapshotReq;

    private static final Logger logger = LoggerFactory.getLogger(CreateSnapshotMetadataStep.class);

    public CreateSnapshotMetadataStep(
        SnapshotDao snapshotDao,
        SnapshotService snapshotService,
        SnapshotRequestModel snapshotReq) {
        this.snapshotDao = snapshotDao;
        this.snapshotService = snapshotService;
        this.snapshotReq = snapshotReq;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        try {
            FlightMap workingMap = context.getWorkingMap();
            Snapshot snapshot = snapshotService.makeSnapshotFromSnapshotRequest(snapshotReq);
            // fill in the project resource that we made in a previous step
            UUID projectResourceId = workingMap.get(SnapshotWorkingMapKeys.PROJECT_RESOURCE_ID, UUID.class);
            snapshot.projectResourceId(projectResourceId);

            UUID snapshotId = snapshotDao.createAndLock(snapshot, context.getFlightId());
            workingMap.put(SnapshotWorkingMapKeys.SNAPSHOT_ID, snapshotId);

            SnapshotSummary snapshotSummary = snapshotDao.retrieveSummaryById(snapshot.getId());
            SnapshotSummaryModel response = snapshotService.makeSummaryModelFromSummary(snapshotSummary);

            FlightUtils.setResponse(context, response, HttpStatus.CREATED);
            return StepResult.getStepResultSuccess();
        } catch (InvalidSnapshotException isEx) {
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, isEx);
        } catch (SnapshotNotFoundException ex) {
            FlightUtils.setErrorResponse(context, ex.toString(), HttpStatus.BAD_REQUEST);
            return new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL, ex);
        }
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        logger.debug("Snapshot creation failed. Deleting metadata.");
        snapshotDao.deleteByName(snapshotReq.getName());
        return StepResult.getStepResultSuccess();
    }

}
