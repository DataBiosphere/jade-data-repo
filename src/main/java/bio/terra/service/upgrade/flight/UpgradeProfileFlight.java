package bio.terra.service.upgrade.flight;

import bio.terra.model.UpgradeModel;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.profile.ProfileService;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class UpgradeProfileFlight extends Flight {

    public UpgradeProfileFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        ApplicationContext appContext = (ApplicationContext) applicationContext;
        ProfileService profileService = (ProfileService) appContext.getBean("profileService");

        UpgradeModel request =
            inputParameters.get(JobMapKeys.REQUEST.getKeyName(), UpgradeModel.class);
        AuthenticatedUserRequest user = inputParameters.get(
            JobMapKeys.AUTH_USER_INFO.getKeyName(), AuthenticatedUserRequest.class);

        addStep(new UpgradeProfileResourcesStep(profileService, request, user));
    }

}
