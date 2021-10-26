package bio.terra.service.profile.flight;

import bio.terra.model.BillingProfileModel;
import bio.terra.service.dataset.flight.ingest.OptionalStep;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.profile.ProfileService;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.StepResult;
import java.util.UUID;
import java.util.function.Predicate;

/**
 * This step is intended to be shared by all flights that are allocating new resources within a
 * billing profile. The step ensures that the caller has proper access to the billing profile and
 * that the reference billing account remains enabled and accessible.
 *
 * <p>It takes the profile id as input. On successful authorization, the associated billing profile
 * is stored in the working map of the flight in the ProfileMapKeys.PROFILE_MODEL entry. On failure,
 * exception is thrown and the flight will fail.
 */
public class AuthorizeBillingProfileUseStep extends OptionalStep {
  private final ProfileService profileService;
  private final UUID profileId;
  private final AuthenticatedUserRequest user;

  public AuthorizeBillingProfileUseStep(
      ProfileService profileService,
      UUID profileId,
      AuthenticatedUserRequest user,
      Predicate<FlightContext> doCondition) {
    super(doCondition);
    this.profileService = profileService;
    this.profileId = profileId;
    this.user = user;
  }

  public AuthorizeBillingProfileUseStep(
      ProfileService profileService, UUID profileId, AuthenticatedUserRequest user) {
    this(profileService, profileId, user, OptionalStep::alwaysDo);
  }

  @Override
  public StepResult doOptionalStep(FlightContext context) {
    BillingProfileModel profileModel = profileService.authorizeLinking(profileId, user);
    FlightMap workingMap = context.getWorkingMap();
    workingMap.put(ProfileMapKeys.PROFILE_MODEL, profileModel);
    return StepResult.getStepResultSuccess();
  }
}
