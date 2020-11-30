package bio.terra.service.profile.flight.delete;

import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class ProfileDeleteFlight extends Flight {

    public ProfileDeleteFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        ApplicationContext appContext = (ApplicationContext) applicationContext;
        ProfileService profileService = (ProfileService) appContext.getBean("profileService");
        ResourceService resourceService = (ResourceService) appContext.getBean("resourceService");

        String profileId = inputParameters.get(ProfileMapKeys.PROFILE_ID, String.class);

        // We do not delete unused Google projects at the point where they become unused; that is, the last
        // file or dataset or snapshot is deleted from them. Instead, we use this profile delete operation
        // to trigger the process of discovering and deleting any unused projects.
        // We do the project delete in three steps:
        //  1. Build the list of projects that use a billing profile. For each project, count any references
        //     to the project. Note that because we do not delete buckets, we will need to examine the dataset_bucket
        //     table to determine whether or not a bucket in a project is no longer in use. If a project is found
        //     to have no references, it (and associated unused buckets) will be marked for delete. From the
        //     perspective of the rest of the system, the buckets and projects no longer exist.
        //     NOTE: we do not undo this. Once these marks are set, the project and bucket will not be
        //     used, even if the projects fail to be deleted.
        //  2. For all projects marked for deletion, actually issue the delete operation to GCP
        //  3. Remove the marked-for-delete objects from the metadata
        // If at that point, the profile still has existing projects, we will error. Otherwise, we
        // complete the deletion of the billing profile.
        //
        // mark unused projects - saves list of marked projects to working map
        // - across many tables - implement in GoogleResourceDao via ResourceService
        // delete marked google projects - gcp deletes, if !allowReuse
        // - ResourceService using GoogleProjectService
        // delete marked project metadata - delete project rows
        // - implement in GoogleResourceDao via ResourceService
        // AND
        // - update getOrCreateBucket and getOrCreateProject to obey the marked for delete flags
        addStep(new DeleteProfileMarkUnusedProjects(resourceService, profileId));
        addStep(new DeleteProfileDeleteUnusedProjects(resourceService));
        addStep(new DeleteProfileProjectMetadata(resourceService));
        addStep(new DeleteProfileAuthzIamStep(profileService, profileId));
        addStep(new DeleteProfileMetadataStep(profileService, profileId));
    }

}
