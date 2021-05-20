package bio.terra.service.snapshot.flight.create;

import bio.terra.app.model.GoogleRegion;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.snapshot.flight.SnapshotWorkingMapKeys;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;

import java.util.UUID;

public class CreateSnapshotGetOrCreateProjectStep implements Step {
    private final ResourceService resourceService;
    private final SnapshotRequestModel snapshotRequestModel;
    private final DatasetService datasetService;
    private final GoogleRegion firestoreRegion;

    public CreateSnapshotGetOrCreateProjectStep(ResourceService resourceService,
                                                SnapshotRequestModel snapshotRequestModel,
                                                DatasetService datasetService,
                                                GoogleRegion firestoreRegion) {
        this.resourceService = resourceService;
        this.snapshotRequestModel = snapshotRequestModel;
        this.datasetService = datasetService;
        this.firestoreRegion = firestoreRegion;
    }

    @Override
    public StepResult doStep(FlightContext context) throws InterruptedException {
        FlightMap workingMap = context.getWorkingMap();
        BillingProfileModel profileModel = workingMap.get(ProfileMapKeys.PROFILE_MODEL, BillingProfileModel.class);
        UUID snapshotId = workingMap.get(SnapshotWorkingMapKeys.SNAPSHOT_ID, UUID.class);

        //TODO - update this comment
        // Since we find projects by their names, this is idempotent. If this step fails and is rerun,
        // Either the project will have been created8and we will find it, or we will create.
        UUID projectResourceId = resourceService.getOrCreateSnapshotProject(profileModel, firestoreRegion);
        workingMap.put(SnapshotWorkingMapKeys.PROJECT_RESOURCE_ID, projectResourceId);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) throws InterruptedException {
        // At this time we do not delete projects, so no undo
        return StepResult.getStepResultSuccess();
    }

}

