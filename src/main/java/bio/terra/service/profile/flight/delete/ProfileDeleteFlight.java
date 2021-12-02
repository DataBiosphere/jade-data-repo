package bio.terra.service.profile.flight.delete;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import java.util.UUID;
import org.springframework.context.ApplicationContext;

public class ProfileDeleteFlight extends Flight {

  public ProfileDeleteFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    ApplicationContext appContext = (ApplicationContext) applicationContext;
    ProfileService profileService = appContext.getBean(ProfileService.class);
    ResourceService resourceService = appContext.getBean(ResourceService.class);

    UUID profileId = inputParameters.get(ProfileMapKeys.PROFILE_ID, UUID.class);

    AuthenticatedUserRequest user =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    var platform =
        CloudPlatformWrapper.of(
            inputParameters.get(JobMapKeys.CLOUD_PLATFORM.getKeyName(), String.class));

    // We do not delete unused Google projects at the point where they become unused; that is, the
    // last
    // file or dataset or snapshot is deleted from them. Instead, we use this profile delete
    // operation
    // to trigger the process of discovering and deleting any unused projects.
    // We do the project delete in three steps:
    //  1. Build the list of projects that use a billing profile. For each project, count any
    // references
    //     to the project. Note that because we do not delete buckets, we will need to examine the
    // dataset_bucket
    //     table to determine whether or not a bucket in a project is no longer in use. If a project
    // is found
    //     to have no references, it (and associated unused buckets) will be marked for delete. From
    // the
    //     perspective of the rest of the system, the buckets and projects no longer exist.
    //     NOTE: we do not undo this. Once these marks are set, the project and bucket will not be
    //     used, even if the projects fail to be deleted. They would be found again and delete
    // attempted
    //     again on a subsequent attempt to delete the profile.
    //  2. For all projects marked for deletion, actually issue the delete operation to GCP. Again,
    // there
    //     is no way to undo here. The projects cannot be recreated. On failure, some might be
    // deleted and
    //     others might not be. We tolerate projects not found during delete.
    //  3. Remove the marked-for-delete objects from the metadata. Again there is no way to rebuild
    // the
    //     deleted state, so no undo. This operation is transactional, so it either all gets done or
    // not.
    // If at that point, the profile still has existing projects, we will error. Otherwise, we
    // complete the deletion of the billing profile.
    // In the case of Azure, metadata records are deleted but will fail if the underlying resources
    // are in use
    if (platform.isGcp()) {
      addStep(new DeleteProfileMarkUnusedProjects(resourceService, profileId));
      addStep(new DeleteProfileDeleteUnusedProjects(resourceService));
      addStep(new DeleteProfileProjectMetadata(resourceService));
    }
    if (platform.isAzure()) {
      addStep(
          new DeleteProfileMarkUnusedApplicationDeployments(
              profileService, resourceService, user, profileId));
      addStep(new DeleteProfileApplicationDeploymentMetadata(resourceService));
    }

    addStep(new DeleteProfileMetadataStep(profileService, profileId));
    addStep(new DeleteProfileAuthzIamStep(profileService, profileId));
  }
}
