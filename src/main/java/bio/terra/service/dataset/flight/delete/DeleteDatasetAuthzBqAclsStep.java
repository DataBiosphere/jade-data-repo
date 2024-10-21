package bio.terra.service.dataset.flight.delete;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DatasetModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.exception.DatasetNotFoundException;
import bio.terra.service.dataset.exception.InvalidDatasetException;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import java.util.Map;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class DeleteDatasetAuthzBqAclsStep implements Step {
  private final IamService sam;
  private final DatasetService datasetService;
  private final ResourceService resourceService;
  private final UUID datasetId;
  private final AuthenticatedUserRequest userReq;

  public DeleteDatasetAuthzBqAclsStep(
      IamService sam,
      DatasetService datasetService,
      ResourceService resourceService,
      UUID datasetId,
      AuthenticatedUserRequest userReq) {
    this.sam = sam;
    this.datasetService = datasetService;
    this.resourceService = resourceService;
    this.datasetId = datasetId;
    this.userReq = userReq;
  }

  private static final Logger logger = LoggerFactory.getLogger(DeleteDatasetAuthzBqAclsStep.class);

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    // TODO: this probably should fail with a 404 before the flight is even attempted
    DatasetModel datasetModel;
    try {
      datasetModel = datasetService.retrieveDatasetModel(datasetId, userReq);
    } catch (DatasetNotFoundException | InvalidDatasetException e) {
      logger.warn("Dataset {} metadata was not found.  Ignoring explicit ACL clear.", datasetId);
      return StepResult.getStepResultSuccess();
    }

    // These policy emails should not change since the dataset is locked by the flight
    Map<IamRole, String> policies =
        sam.retrievePolicyEmails(userReq, IamResourceType.DATASET, datasetId);

    // The underlying service provides retries so we do not need to retry this operation
    resourceService.revokePoliciesBqJobUser(datasetModel.getDataProject(), policies.values());

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    // can't undo delete
    logger.warn("Trying to undo clear ACLs for dataset {}", datasetId);
    return StepResult.getStepResultSuccess();
  }
}
