package bio.terra.service.snapshot.flight.create;

import bio.terra.service.auth.iam.IamRole;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class SnapshotAuthzBqJobUserStep implements Step {
  private final SnapshotService snapshotService;
  private final ResourceService resourceService;
  private final String snapshotName;
  private final Dataset sourceDataset;

  public SnapshotAuthzBqJobUserStep(
      SnapshotService snapshotService,
      ResourceService resourceService,
      String snapshotName,
      Dataset sourceDataset) {
    this.snapshotService = snapshotService;
    this.resourceService = resourceService;
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
    List<String> policyEmails =
        new ArrayList<>(List.of(policyMap.get(IamRole.STEWARD), policyMap.get(IamRole.READER)));

    if (sourceDataset.isInheritSteward()) {
      Map<IamRole, String> sourceDatasetPolicyMap =
          workingMap.get(
              SnapshotWorkingMapKeys.SOURCE_DATASET_POLICY_MAP, new TypeReference<>() {});
      // Allow the custodian to make queries in this project.
      policyEmails.add(sourceDatasetPolicyMap.get(IamRole.CUSTODIAN));
    }
    // The underlying service provides retries so we do not need to retry this operation
    resourceService.grantPoliciesBqJobUser(googleProjectId, policyEmails);

    return StepResult.getStepResultSuccess();
  }

  @Override
  public StepResult undoStep(FlightContext context) {
    return StepResult.getStepResultSuccess();
  }
}
