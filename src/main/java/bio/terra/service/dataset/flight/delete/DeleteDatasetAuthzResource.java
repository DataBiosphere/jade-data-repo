package bio.terra.service.dataset.flight.delete;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.flight.create.CreateDatasetAuthzBqJobUserStep;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteDatasetAuthzResource implements Step {
  private IamService sam;
  private UUID datasetId;
  private AuthenticatedUserRequest userReq;

  public DeleteDatasetAuthzResource(
      IamService sam, UUID datasetId, AuthenticatedUserRequest userReq) {
    this.sam = sam;
    this.datasetId = datasetId;
    this.userReq = userReq;
  }

  private static Logger logger = LoggerFactory.getLogger(CreateDatasetAuthzBqJobUserStep.class);

  @Override
  public StepResult doStep(FlightContext context) {
    sam.deleteDatasetResource(userReq, datasetId);
    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // can't undo delete
    logger.warn("Trying to undo delete resource for dataset " + datasetId.toString());
    return StepResult.getStepResultSuccess();
  }
}
