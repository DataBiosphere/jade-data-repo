package bio.terra.service.profile;

import bio.terra.app.controller.exception.ValidationException;
import bio.terra.app.utils.PolicyUtils;
import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.ValidationUtils;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.BillingProfileUpdateModel;
import bio.terra.model.CloudPlatform;
import bio.terra.model.EnumerateBillingProfileModel;
import bio.terra.model.PolicyMemberRequest;
import bio.terra.model.PolicyModel;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.exception.IamUnauthorizedException;
import bio.terra.service.job.JobMapKeys;
import bio.terra.service.job.JobService;
import bio.terra.service.profile.azure.AzureAuthzService;
import bio.terra.service.profile.exception.ProfileNotFoundException;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.profile.flight.create.ProfileCreateFlight;
import bio.terra.service.profile.flight.delete.ProfileDeleteFlight;
import bio.terra.service.profile.flight.update.ProfileUpdateFlight;
import bio.terra.service.profile.google.GoogleBillingService;
import bio.terra.service.resourcemanagement.exception.InaccessibleApplicationDeploymentException;
import bio.terra.service.resourcemanagement.exception.InaccessibleBillingAccountException;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class ProfileService {
  private static final Logger logger = LoggerFactory.getLogger(ProfileService.class);

  private final ProfileDao profileDao;
  private final IamService iamService;
  private final JobService jobService;
  private final GoogleBillingService googleBillingService;
  private final AzureAuthzService azureAuthzService;

  @Autowired
  public ProfileService(
      ProfileDao profileDao,
      IamService iamService,
      JobService jobService,
      GoogleBillingService googleBillingService,
      AzureAuthzService azureAuthzService) {
    this.profileDao = profileDao;
    this.iamService = iamService;
    this.jobService = jobService;
    this.googleBillingService = googleBillingService;
    this.azureAuthzService = azureAuthzService;
  }

  /**
   * Create a new billing profile providing a valid google billing account We make the following
   * checks:
   *
   * <ul>
   *   <li>The service must have proper permissions on the google billing account
   *   <li>The caller must have billing.resourceAssociation.create permission on the google billing
   *       account
   *   <li>The google billing account must be enabled
   * </ul>
   *
   * <p>The billing profile name does not need to be unique across all billing profiles. The billing
   * profile id needs to be a unique and valid uuid
   *
   * @param billingProfileRequest the request to create a billing profile
   * @return jobId of the submitted stairway job
   */
  public String createProfile(
      BillingProfileRequestModel billingProfileRequest, AuthenticatedUserRequest user) {
    String description =
        String.format("Create billing profile '%s'", billingProfileRequest.getProfileName());
    return jobService
        .newJob(description, ProfileCreateFlight.class, billingProfileRequest, user)
        .submit();
  }

  /**
   * Update billing profile. We make the following checks:
   *
   * <ul>
   *   <li>The service must have proper permissions on the google billing account
   *   <li>The caller must have billing.resourceAssociation.create permission on the google billing
   *       account
   *   <li>The google billing account must be enabled
   * </ul>
   *
   * @param billingProfileRequest request with changes to billing profile
   * @param user the user attempting to update the billing profile
   * @return jobId of the submitted stairway job
   */
  public String updateProfile(
      BillingProfileUpdateModel billingProfileRequest, AuthenticatedUserRequest user) {
    iamService.verifyAuthorization(
        user,
        IamResourceType.SPEND_PROFILE,
        billingProfileRequest.getId().toString(),
        IamAction.UPDATE_BILLING_ACCOUNT);

    String description =
        String.format("Update billing for profile id '%s'", billingProfileRequest.getId());
    return jobService
        .newJob(description, ProfileUpdateFlight.class, billingProfileRequest, user)
        .addParameter(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.SPEND_PROFILE)
        .addParameter(
            JobMapKeys.IAM_RESOURCE_ID.getKeyName(), billingProfileRequest.getId().toString())
        .addParameter(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.UPDATE_BILLING_ACCOUNT)
        .submit();
  }

  /**
   * Remove billing profile. We make the following checks:
   *
   * <ul>
   *   <li>the caller must be an owner of the billing profile
   *   <li>There must be no dependencies on the billing profile; that is, no snapshots, dataset, or
   *       buckets referencing the profile
   * </ul>
   *
   * @param id the unique id of the bill profile
   * @param user the user attempting the delete
   * @return jobId of the submitted stairway job
   */
  public String deleteProfile(UUID id, AuthenticatedUserRequest user) {
    iamService.verifyAuthorization(
        user, IamResourceType.SPEND_PROFILE, id.toString(), IamAction.DELETE);
    CloudPlatform platform;
    try {
      BillingProfileModel billingProfile = profileDao.getBillingProfileById(id);
      platform = billingProfile.getCloudPlatform();
    } catch (ProfileNotFoundException e) {
      // Use default cloud profile if the billing profile does not exist and let the flight handle
      // the object
      // not being found
      platform = CloudPlatformWrapper.DEFAULT;
    }

    String description = String.format("Delete billing profile id '%s'", id);
    return jobService
        .newJob(description, ProfileDeleteFlight.class, null, user)
        .addParameter(ProfileMapKeys.PROFILE_ID, id)
        .addParameter(JobMapKeys.CLOUD_PLATFORM.getKeyName(), platform.name())
        .addParameter(JobMapKeys.IAM_RESOURCE_TYPE.getKeyName(), IamResourceType.SPEND_PROFILE)
        .addParameter(JobMapKeys.IAM_RESOURCE_ID.getKeyName(), id)
        .addParameter(JobMapKeys.IAM_ACTION.getKeyName(), IamAction.DELETE)
        .submit();
  }

  /**
   * Enumerate the profiles that are visible to the requesting user
   *
   * @param offset start of the range of profiles to return for this request
   * @param limit maximum number of profiles to return in this request
   * @param user user on whose behalf we are making this request
   * @return enumeration profile containing the list and total
   */
  public EnumerateBillingProfileModel enumerateProfiles(
      Integer offset, Integer limit, AuthenticatedUserRequest user) {
    Set<UUID> resources =
        iamService.listAuthorizedResources(user, IamResourceType.SPEND_PROFILE).keySet();
    if (resources.isEmpty()) {
      return new EnumerateBillingProfileModel().total(0).items(List.of());
    }
    return profileDao.enumerateBillingProfiles(offset, limit, resources);
  }

  /**
   * Lookup a billing profile by the profile id with auth check. Supports the REST API
   *
   * @param id the unique idea of this billing profile
   * @param user authenticated user
   * @return On success, the billing profile model
   * @throws ProfileNotFoundException when the profile is not found
   * @throws IamUnauthorizedException when the caller does not have access to the billing profile
   */
  public BillingProfileModel getProfileById(UUID id, AuthenticatedUserRequest user) {
    if (!iamService.hasAnyActions(user, IamResourceType.SPEND_PROFILE, id.toString())) {
      throw new IamUnauthorizedException("unauthorized");
    }
    return getProfileByIdNoCheck(id);
  }

  /**
   * Lookup a billing profile by the profile id with no auth check. Used for internal references.
   *
   * @param id the unique idea of this billing profile
   * @return On success, the billing profile model
   * @throws ProfileNotFoundException when the profile is not found
   */
  public BillingProfileModel getProfileByIdNoCheck(UUID id) {
    return profileDao.getBillingProfileById(id);
  }

  // The idea is to use this call from create snapshot and create asset to validate that the
  // billing account is usable by the calling user

  /**
   * Called by services to verify that a profile exists, that the user has the link permission on
   * the profile, that the underlying billing account is usable, and that there is a path of
   * delegation to the user. The path of delegation is formed by one of the owners of the billing
   * profile having "create link" permission on the billing account.
   *
   * @param profileId the profile id to attempt to authorize
   * @param user the user attempting associate some object with the profile
   * @return the profile model associated with the profile id
   */
  public BillingProfileModel authorizeLinking(UUID profileId, AuthenticatedUserRequest user) {
    logger.info("Verify authorization for link id={} user={}", profileId, user.getEmail());
    iamService.verifyAuthorization(
        user, IamResourceType.SPEND_PROFILE, profileId.toString(), IamAction.LINK);
    return profileDao.getBillingProfileById(profileId);
  }

  public PolicyModel addProfilePolicyMember(
      UUID profileId,
      String policyName,
      PolicyMemberRequest policyMember,
      AuthenticatedUserRequest user) {
    return iamService.addPolicyMember(
        user, IamResourceType.SPEND_PROFILE, profileId, policyName, policyMember.getEmail());
  }

  public PolicyModel deleteProfilePolicyMember(
      UUID profileId, String policyName, String memberEmail, AuthenticatedUserRequest user) {
    logger.info(
        "id={} policy={} email={} authuser={}",
        profileId,
        policyName,
        memberEmail,
        user.getEmail());
    // member email can't be null since it is part of the URL
    if (!ValidationUtils.isValidEmail(memberEmail)) {
      throw new ValidationException("InvalidMemberEmail");
    }

    return iamService.deletePolicyMember(
        user, IamResourceType.SPEND_PROFILE, profileId, policyName, memberEmail);
  }

  public List<PolicyModel> retrieveProfilePolicies(UUID profileId, AuthenticatedUserRequest user) {
    return PolicyUtils.samToTdrPolicyModels(
        iamService.retrievePolicies(user, IamResourceType.SPEND_PROFILE, profileId));
  }

  // -- methods invoked from billing profile flights --

  public BillingProfileModel createProfileMetadata(
      BillingProfileRequestModel profileRequest, AuthenticatedUserRequest user) {
    return profileDao.createBillingProfile(profileRequest, user.getEmail());
  }

  public BillingProfileModel updateProfileMetadata(BillingProfileUpdateModel profileRequest) {
    return profileDao.updateBillingProfileById(profileRequest);
  }

  public boolean deleteProfileMetadata(UUID profileId) {
    // TODO: refuse to delete if there are dependent projects
    return profileDao.deleteBillingProfileById(profileId);
  }

  public void createProfileIamResource(
      BillingProfileRequestModel request, AuthenticatedUserRequest user) {
    iamService.createProfileResource(user, request.getId().toString());
  }

  public void deleteProfileIamResource(UUID profileId, AuthenticatedUserRequest user) {
    iamService.deleteProfileResource(user, profileId.toString());
  }

  // Verify user access to the billing account during billing profile creation
  public void verifyAccount(String billingAccountId, AuthenticatedUserRequest user) {
    if (!googleBillingService.canAccess(user, billingAccountId)) {
      throw new InaccessibleBillingAccountException(
          "The user '"
              + user.getEmail()
              + "' needs access to billing account '"
              + billingAccountId
              + "' to perform the requested operation");
    }
  }

  // Verify user access to the deployed application during billing profile creation
  public void verifyDeployedApplication(
      UUID subscriptionId,
      String resourceGroupName,
      String applicationDeploymentName,
      AuthenticatedUserRequest user) {
    if (!azureAuthzService.canAccess(
        user, subscriptionId, resourceGroupName, applicationDeploymentName)) {
      throw new InaccessibleApplicationDeploymentException(
          "The user '"
              + user.getEmail()
              + "' needs access to deployed application '"
              + applicationDeploymentName
              + "' to perform the requested "
              + "operation");
    }
  }
}
