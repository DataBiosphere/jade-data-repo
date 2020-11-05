package bio.terra.service.profile.flight.delete;

import bio.terra.service.iam.IamAction;
import bio.terra.service.iam.IamProviderInterface;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.flight.VerifyAuthorizationStep;
import bio.terra.service.profile.ProfileService;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.stairway.Flight;
import bio.terra.stairway.FlightMap;
import org.springframework.context.ApplicationContext;

public class ProfileDeleteFlight extends Flight {

    public ProfileDeleteFlight(FlightMap inputParameters, Object applicationContext) {
        super(inputParameters, applicationContext);

        ApplicationContext appContext = (ApplicationContext) applicationContext;
        ProfileService profileService = (ProfileService) appContext.getBean("profileService");
        IamProviderInterface iamClient = (IamProviderInterface) appContext.getBean("iamProvider");

        String profileId = inputParameters.get(ProfileMapKeys.PROFILE_ID, String.class);

        addStep(new VerifyAuthorizationStep(
            iamClient,
            IamResourceType.SPEND_PROFILE,
            profileId,
            IamAction.DELETE));

        addStep(new DeleteProfileAuthzIamStep(profileService, profileId));
        addStep(new DeleteProfileMetadataStep(profileService, profileId));
    }

}
