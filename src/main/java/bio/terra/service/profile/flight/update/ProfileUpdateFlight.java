package bio.terra.service.profile.flight.update;

import bio.terra.model.BillingProfileUpdateModel;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class ProfileUpdateFlight extends Flight {

    public ProfileUpdateFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        ApplicationContext appContext = (ApplicationContext) applicationContext;
        ProfileService profileService = (ProfileService) appContext.getBean("profileService");
        GoogleProjectService googleProjectService =
            (GoogleProjectService) appContext.getBean("googleProjectService");

        BillingProfileUpdateModel request =
            inputParameters.get(JobMapKeys.REQUEST.getKeyName(), bio.terra.model.BillingProfileUpdateModel.class);

        AuthenticatedUserRequest user = inputParameters.get(
            JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

        addStep(new UpdateProfileRetrieveExistingProfileStep(profileService, request, user));
        //update billing account id metadata
        addStep(new UpdateProfileMetadataStep(profileService, request, user));
        // Make sure valid account before changing in gcloud project
        addStep(new UpdateProfileVerifyAccountStep(profileService, user));
        // Update billing profile in gcloud project
        addStep(new UpdateProfileUpdateGCloudProject(googleProjectService));
    }

}
