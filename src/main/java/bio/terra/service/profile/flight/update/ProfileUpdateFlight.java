package bio.terra.service.profile.flight.update;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileUpdateModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.common.JournalRecordUpdateEntryStep;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.journal.JournalService;
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
    JournalService journalService = appContext.getBean(JournalService.class);

    BillingProfileUpdateModel request =
        inputParameters.get(
            JobMapKeys.REQUEST.getKeyName(), bio.terra.model.BillingProfileUpdateModel.class);

    AuthenticatedUserRequest user =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    addStep(new UpdateProfileRetrieveExistingProfileStep(profileService, request, user));
    // update billing account id metadata
    addStep(new UpdateProfileMetadataStep(profileService, request, user));
    // Make sure valid account before changing in gcloud project
    addStep(new UpdateProfileVerifyAccountStep(profileService, user));
    // Update billing profile in gcloud project
    addStep(new UpdateProfileUpdateGCloudProject(googleProjectService));
    addStep(
        new JournalRecordUpdateEntryStep(
            journalService,
            user,
            request.getId(),
            IamResourceType.SPEND_PROFILE,
            "Updated billing profile."));
  }
}
