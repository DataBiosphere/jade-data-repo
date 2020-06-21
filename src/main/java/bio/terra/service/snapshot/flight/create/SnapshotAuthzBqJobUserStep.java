package bio.terra.service.snapshot.flight.create;

import bio.terra.service.resourcemanagement.DataLocationService;
import bio.terra.service.resourcemanagement.google.GoogleResourceService;
import bio.terra.service.snapshot.Snapshot;
import bio.terra.service.snapshot.SnapshotDataProject;
import bio.terra.service.snapshot.SnapshotService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.List;

public class SnapshotAuthzBqJobUserStep implements Step {
    private final SnapshotService snapshotService;
    private final GoogleResourceService resourceService;
    private final DataLocationService dataLocationService;
    private final String snapshotName;

    public SnapshotAuthzBqJobUserStep(
        SnapshotService snapshotService,
        DataLocationService dataLocationService,
        GoogleResourceService resourceService,
        String snapshotName) {
        this.snapshotService = snapshotService;
        this.resourceService = resourceService;
        this.dataLocationService = dataLocationService;
        this.snapshotName = snapshotName;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        FlightMap workingMap = context.getWorkingMap();
        Snapshot snapshot = snapshotService.retrieveByName(snapshotName);
        SnapshotDataProject projectForSnapshot = dataLocationService.getOrCreateProject(snapshot);

        List<String> policyEmails = workingMap.get(SnapshotWorkingMapKeys.POLICY_EMAIL, List.class);

        // The underlying service provides retries so we do not need to retry this operation
        resourceService.grantPoliciesBqJobUser(projectForSnapshot.getGoogleProjectId(), policyEmails);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) throws InterruptedException {
        // TODO:
        return StepResult.getStepResultSuccess();
    }
}
