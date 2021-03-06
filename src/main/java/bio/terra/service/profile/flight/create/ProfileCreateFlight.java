package bio.terra.service.profile.flight.create;

import bio.terra.model.BillingProfileRequestModel;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.ProfileService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class ProfileCreateFlight extends Flight {

    public ProfileCreateFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        ApplicationContext appContext = (ApplicationContext) applicationContext;
        ProfileService profileService = (ProfileService) appContext.getBean("profileService");

        BillingProfileRequestModel request =
            inputParameters.get(JobMapKeys.REQUEST.getKeyName(), BillingProfileRequestModel.class);

        AuthenticatedUserRequest user = inputParameters.get(
            JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

        addStep(new CreateProfileMetadataStep(profileService, request, user));
        addStep(new CreateProfileVerifyAccountStep(profileService, request, user));
        addStep(new CreateProfileAuthzIamStep(profileService, request, user));
    }

}
