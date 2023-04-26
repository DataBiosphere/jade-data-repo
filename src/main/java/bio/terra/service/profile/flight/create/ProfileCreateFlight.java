package bio.terra.service.profile.flight.create;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.journal.JournalService;
import bio.terra.service.profile.ProfileService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class ProfileCreateFlight extends Flight {

  public ProfileCreateFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    ApplicationContext appContext = (ApplicationContext) applicationContext;
    ProfileService profileService = appContext.getBean(ProfileService.class);
    JournalService journalService = appContext.getBean(JournalService.class);

    BillingProfileRequestModel request =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), BillingProfileRequestModel.class);

    AuthenticatedUserRequest user =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    CloudPlatformWrapper platform = CloudPlatformWrapper.of(request.getCloudPlatform());

    addStep(new GetOrCreateProfileIdStep(request));
    addStep(new CreateProfileMetadataStep(profileService, request, user));
    if (platform.isGcp()) {
      addStep(new CreateProfileVerifyAccountStep(profileService, request, user));
    }
    if (platform.isAzure()) {
      addStep(new CreateProfileVerifyDeployedApplicationStep(profileService, request, user));
    }
    addStep(new CreateProfileAuthzIamStep(profileService, request, user));
    addStep(
        new CreateProfileJournalEntryStep(
            journalService,
            user,
            IamResourceType.SPEND_PROFILE,
            "Billing profile created.",
            true,
            request));
  }
}
