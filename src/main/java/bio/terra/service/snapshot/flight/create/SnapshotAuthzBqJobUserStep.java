package bio.terra.service.snapshot.flight.create;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.List;
import java.util.Map;

public class SnapshotAuthzBqJobUserStep implements Step {
  private final SnapshotService snapshotService;
  private final ResourceService resourceService;
  private final IamService sam;
  private final AuthenticatedUserRequest request;
  private final String snapshotName;
  private final Dataset sourceDataset;

  public SnapshotAuthzBqJobUserStep(
      SnapshotService snapshotService,
      ResourceService resourceService,
      IamService sam,
      AuthenticatedUserRequest request,
      String snapshotName,
      Dataset sourceDataset) {
    this.snapshotService = snapshotService;
    this.resourceService = resourceService;
    this.sam = sam;
    this.request = request;
    this.snapshotName = snapshotName;
    this.sourceDataset = sourceDataset;
  }

  @Override
  public StepResult doStep(FlightContext context) throws InterruptedException {
    FlightMap workingMap = context.getWorkingMap();
    Map<IamRole, String> policyMap =
        workingMap.get(SnapshotWorkingMapKeys.POLICY_MAP, new TypeReference<>() {});

    String googleProjectId =
        snapshotService.retrieveByName(snapshotName).getProjectResource().getGoogleProjectId();

    // Allow the steward and reader to make queries in this project.
    // The underlying service provides retries so we do not need to retry this operation
    resourceService.grantPoliciesBqJobUser(
        googleProjectId, List.of(policyMap.get(IamRole.STEWARD)));
    resourceService.grantPoliciesBqJobUser(googleProjectId, List.of(policyMap.get(IamRole.READER)));

    Boolean inheritEnabled =
        context
            .getInputParameters()
            .get(SnapshotWorkingMapKeys.SNAPSHOT_INHERIT_STEWARD_ENABLED, Boolean.class);
    if (inheritEnabled != null && inheritEnabled) {
      var datasetPolicyMap =
          sam.retrievePolicyEmails(request, IamResourceType.DATASET, sourceDataset.getId());
      // Allow the custodian to make queries in this project.
      resourceService.grantPoliciesBqJobUser(
          googleProjectId, List.of(datasetPolicyMap.get(IamRole.CUSTODIAN)));
    }

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) throws InterruptedException {
    return StepResult.getStepResultSuccess();
  }
}
