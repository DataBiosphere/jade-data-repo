package bio.terra.datarepo.service.profile.flight.create;

import bio.terra.datarepo.common.CloudPlatformWrapper;
import bio.terra.datarepo.model.BillingProfileRequestModel;
import bio.terra.datarepo.service.iam.AuthenticatedUserRequest;
import bio.terra.datarepo.service.job.JobMapKeys;
import bio.terra.datarepo.service.profile.ProfileService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class ProfileCreateFlight extends Flight {

  public ProfileCreateFlight(FlightMap inputParameters, Object applicationContext) {
    super(inputParameters, applicationContext);

    ApplicationContext appContext = (ApplicationContext) applicationContext;
    ProfileService profileService = appContext.getBean(ProfileService.class);

    BillingProfileRequestModel request =
        inputParameters.get(JobMapKeys.REQUEST.getKeyName(), BillingProfileRequestModel.class);

    AuthenticatedUserRequest user =
        inputParameters.get(JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

    addStep(new CreateProfileMetadataStep(profileService, request, user));
    addStep(new CreateProfileVerifyAccountStep(profileService, request, user));
    if (CloudPlatformWrapper.of(request.getCloudPlatform()).isAzure()) {
      addStep(new CreateProfileVerifyDeployedApplicationStep(profileService, request, user));
    }
    addStep(new CreateProfileAuthzIamStep(profileService, request, user));
  }
}
