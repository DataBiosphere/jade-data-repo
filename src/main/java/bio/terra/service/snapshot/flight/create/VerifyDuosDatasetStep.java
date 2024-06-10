package bio.terra.service.snapshot.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.duos.DuosClient;
import bio.terra.service.job.DefaultUndoStep;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.UUID;

public class VerifyDuosDatasetStep extends DefaultUndoStep {
  private final DuosClient duosClient;
  private final String duosId;
  private final AuthenticatedUserRequest user;

  public VerifyDuosDatasetStep(DuosClient duosClient, String duosId, AuthenticatedUserRequest user) {
    this.duosClient = duosClient;
    this.duosId = duosId;
    this.user = user;
  }
  @Override
  public StepResult doStep(FlightContext context) {
    if (duosId != null) {
      // We fetch the DUOS dataset to confirm its existence, but do not need the returned value.
      duosClient.getDataset(duosId, user);
    }
    return StepResult.getStepResultSuccess();
  }

}
