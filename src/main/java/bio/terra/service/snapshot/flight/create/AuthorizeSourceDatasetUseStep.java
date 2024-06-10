package bio.terra.service.snapshot.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.UUID;

public class AuthorizeSourceDatasetUseStep extends DefaultUndoStep {
  private final IamService iamService;
  private final UUID datasetId;
  private final AuthenticatedUserRequest user;

  public AuthorizeSourceDatasetUseStep(IamService iamService, UUID datasetId, AuthenticatedUserRequest user) {
    this.iamService = iamService;
    this.datasetId = datasetId;
    this.user = user;
  }

  @Override
  public StepResult doStep(FlightContext context) {
    iamService.verifyAuthorization(user, IamResourceType.DATASET, datasetId.toString(), IamAction.LINK_SNAPSHOT);
    return StepResult.getStepResultSuccess();
  }
}
