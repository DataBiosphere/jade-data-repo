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
    ProfileService profileService = appContext.getBean(ProfileService.class);
    GoogleProjectService googleProjectService = appContext.getBean(GoogleProjectService.class);

    BillingProfileUpdateModel request = JobMapKeys.REQUEST.get(inputParameters);

    AuthenticatedUserRequest user = JobMapKeys.AUTH_USER_INFO.get(inputParameters);

    addStep(new UpdateProfileRetrieveExistingProfileStep(profileService, request));
    // update billing account id metadata
    addStep(new UpdateProfileMetadataStep(profileService, request));
    // Make sure valid account before changing in gcloud project
    addStep(new UpdateProfileVerifyAccountStep(profileService, user));
    // Update billing profile in gcloud project
    addStep(new UpdateProfileUpdateGCloudProject(googleProjectService));
  }
}
