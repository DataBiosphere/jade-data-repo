package bio.terra.service.profile.flight.delete;

import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.UUID;

public class DeleteProfileMarkUnusedApplicationDeployments implements Step {

    private final ProfileService profileService;
    private final ResourceService resourceService;
    private final AuthenticatedUserRequest user;
    private final UUID profileId;

    private static final Logger logger = LoggerFactory.getLogger(DeleteProfileMarkUnusedApplicationDeployments.class);

    public DeleteProfileMarkUnusedApplicationDeployments(ProfileService profileService,
                                                         ResourceService resourceService,
                                                         AuthenticatedUserRequest user,
                                                         UUID profileId) {
        this.profileService = profileService;
        this.resourceService = resourceService;
        this.user = user;
        this.profileId = profileId;
    }

    @Override
    public StepResult doStep(FlightContext context) {
        FlightMap workingMap = context.getWorkingMap();
        List<UUID> appIdList = resourceService.markUnusedApplicationDeploymentsForDelete(profileId);
        workingMap.put(ProfileMapKeys.PROFILE_APPLICATION_DEPLOYMENT_ID_LIST, appIdList);
        return StepResult.getStepResultSuccess();
    }

    @Override
    public StepResult undoStep(FlightContext context) {
        return StepResult.getStepResultSuccess();
    }
}
