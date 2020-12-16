package bio.terra.service.profile;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.BillingProfileUpdateModel;
import bio.terra.model.EnumerateBillingProfileModel;
import bio.terra.model.PolicyMemberRequest;
import bio.terra.model.PolicyModel;
import bio.terra.model.UpgradeModel;
import bio.terra.model.UpgradeResponseModel;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamAction;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamService;
import bio.terra.service.iam.exception.IamNotFoundException;
import bio.terra.service.iam.exception.IamUnauthorizedException;
import bio.terra.service.job.JobService;
import bio.terra.service.profile.exception.ProfileNotFoundException;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.profile.flight.create.ProfileCreateFlight;
import bio.terra.service.profile.flight.delete.ProfileDeleteFlight;
import bio.terra.service.profile.flight.update.ProfileUpdateFlight;
import bio.terra.service.profile.google.GoogleBillingService;
import bio.terra.service.resourcemanagement.exception.InaccessibleBillingAccountException;
import org.broadinstitute.dsde.workbench.client.sam.ApiException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.stream.Collectors;

@Component
public class ProfileService {
    private static final Logger logger = LoggerFactory.getLogger(ProfileService.class);

    private final ProfileDao profileDao;
    private final IamService iamService;
    private final JobService jobService;
    private final GoogleBillingService billingService;
    private final ApplicationConfiguration applicationConfiguration;

    @Autowired
    public ProfileService(ProfileDao profileDao,
                          IamService iamService,
                          JobService jobService,
                          GoogleBillingService billingService,
                          ApplicationConfiguration applicationConfiguration) {
        this.profileDao = profileDao;
        this.iamService = iamService;
        this.jobService = jobService;
        this.billingService = billingService;
        this.applicationConfiguration = applicationConfiguration;
    }

    /**
     * Create a new billing profile providing an valid google billing account
     * We make the following checks:
     * <ul>
     *     <li>The service must have proper permissions on the google billing account</li>
     *     <li>The caller must have billing.resourceAssociation.create permission on the google billing account</li>
     *     <li>The google billing account must be enabled</li>
     * </ul>
     * <p>
     * The billing profile name does not need to be unique across all billing profiles.
     * The billing profile id needs to be a unique and valid uuid
     *
     * @param billingProfileRequest the request to create a billing profile
     * @return jobId of the submitted stairway job
     */
    public String createProfile(BillingProfileRequestModel billingProfileRequest,
                                AuthenticatedUserRequest user) {
        String description = String.format("Create billing profile '%s'", billingProfileRequest.getProfileName());
        return jobService
            .newJob(description, ProfileCreateFlight.class, billingProfileRequest, user)
            .submit();
    }

    /**
     * Update billing profile. We make the following checks:
     * <ul>
     *     <li>The service must have proper permissions on the google billing account</li>
     *     <li>The caller must have billing.resourceAssociation.create permission on the google billing account</li>
     *     <li>The google billing account must be enabled</li>
     * </ul>
     *
     * @param  billingProfileRequest request with changes to billing profile
     * @param user the user attempting to update the billing profile
     * @return jobId of the submitted stairway job
     */
    public String updateProfile(BillingProfileUpdateModel billingProfileRequest,
                                AuthenticatedUserRequest user) {
        iamService.verifyAuthorization(user, IamResourceType.SPEND_PROFILE, billingProfileRequest.getId(),
            IamAction.UPDATE_BILLING_ACCOUNT);

        String description = String.format("Update billing for profile id '%s'", billingProfileRequest.getId());
        return jobService
            .newJob(description, ProfileUpdateFlight.class, billingProfileRequest, user)
            .submit();
    }

    /**
     * Remove billing profile. We make the following checks:
     * <ul>
     *     <li>the caller must be an owner of the billing profile</li>
     *     <li>There must be no dependencies on the billing profile;
     *     that is, no snapshots, dataset, or buckets referencing the profile</li>
     * </ul>
     *
     * @param id the unique id of the bill profile
     * @param user the user attempting the delete
     * @return jobId of the submitted stairway job
     */
    public String deleteProfile(String id, AuthenticatedUserRequest user) {
        if (applicationConfiguration.isEnforceBillingProfileAuthorization()) {
            iamService.verifyAuthorization(user, IamResourceType.SPEND_PROFILE, id, IamAction.DELETE);
        }

        String description = String.format("Delete billing profile id '%s'", id);
        return jobService
            .newJob(description, ProfileDeleteFlight.class, null, user)
            .addParameter(ProfileMapKeys.PROFILE_ID, id)
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
    public EnumerateBillingProfileModel enumerateProfiles(Integer offset,
                                                          Integer limit,
                                                          AuthenticatedUserRequest user) {
        if (applicationConfiguration.isEnforceBillingProfileAuthorization()) {
            List<UUID> resources = iamService.listAuthorizedResources(user, IamResourceType.SPEND_PROFILE);
            if (resources.isEmpty()) {
                return new EnumerateBillingProfileModel().total(0);
            }
            return profileDao.enumerateBillingProfiles(offset, limit, resources);
        }
        return new EnumerateBillingProfileModel().total(0);
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
    public BillingProfileModel getProfileById(String id, AuthenticatedUserRequest user) {
        if (applicationConfiguration.isEnforceBillingProfileAuthorization()) {
            if (!iamService.hasActions(user, IamResourceType.SPEND_PROFILE, id)) {
                throw new IamUnauthorizedException("unauthorized");
            }
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
    public BillingProfileModel getProfileByIdNoCheck(String id) {
        UUID profileId = UUID.fromString(id);
        return profileDao.getBillingProfileById(profileId);
    }


    // The idea is to use this call from create snapshot and create asset to validate that the
    // billing account is usable by the calling user

    /**
     * Called by services to verify that a profile exists, that the user has the link permission
     * on the profile, that the underlying billing account is usable, and that there is a path of
     * delegation to the user. The path of delegation is formed by one of the owners of the billing profile
     * having "create link" permission on the billing account.
     *
     * @param profileId the profile id to attempt to authorize
     * @param user the user attempting associate some object with the profile
     * @return the profile model associated with the profile id
     */
    public BillingProfileModel authorizeLinking(UUID profileId, AuthenticatedUserRequest user) {
        if (applicationConfiguration.isEnforceBillingProfileAuthorization()) {
            iamService.verifyAuthorization(user,
                IamResourceType.SPEND_PROFILE,
                profileId.toString(),
                IamAction.LINK);
        }
        BillingProfileModel profileModel = profileDao.getBillingProfileById(profileId);

        // TODO: check bill account usable and validate delegation path
        //  For now we just make sure that the building account is accessible to the
        //  TDR service account.
        String billingAccountId = profileModel.getBillingAccountId();
        if (!billingService.repositoryCanAccess(billingAccountId)) {
            throw new InaccessibleBillingAccountException("The repository needs access to billing account "
                + billingAccountId + " to perform the requested operation");
        }

        return profileModel;
    }

    public PolicyModel addProfilePolicyMember(String profileId,
                                              String policyName,
                                              PolicyMemberRequest policyMember,
                                              AuthenticatedUserRequest user) {
        // TODO: their may may not be a resource behind this profile. Therefore we
        //  eat any unauthorize/forbidden exception called that a success.
        //  Remove when we enable authorization.
        try {
            return iamService.addPolicyMember(
                user,
                IamResourceType.SPEND_PROFILE,
                UUID.fromString(profileId),
                policyName,
                policyMember.getEmail());
        } catch (IamUnauthorizedException ex) {
            // TODO: If we are not enforcing authorization then we allow there to be a missing profile
            //  sam resource. Remove during clean up.
            if (!applicationConfiguration.isEnforceBillingProfileAuthorization()) {
                if (ex.getCause() instanceof ApiException) {
                    ApiException samEx = (ApiException) ex.getCause();
                    if (samEx.getCode() == 403) {
                        logger.warn("Ignoring not found exception", ex);
                        return new PolicyModel();
                    }
                }
            }
            throw ex;
        }
    }

    // -- methods invoked from billing profile flights --

    public BillingProfileModel createProfileMetadata(BillingProfileRequestModel profileRequest,
                                                     AuthenticatedUserRequest user) {
        return profileDao.createBillingProfile(profileRequest, user.getEmail());
    }

    public BillingProfileModel updateProfileMetadata(BillingProfileUpdateModel profileRequest) {
        return profileDao.updateBillingProfileById(profileRequest);
    }

    public boolean deleteProfileMetadata(String profileId) {
        // TODO: refuse to delete if there are dependent projects
        UUID profileUuid = UUID.fromString(profileId);
        return profileDao.deleteBillingProfileById(profileUuid);
    }

    public void createProfileIamResource(BillingProfileRequestModel request, AuthenticatedUserRequest user) {
        iamService.createProfileResource(user, request.getId());
    }

    public void deleteProfileIamResource(String profileId, AuthenticatedUserRequest user) {
        // TODO: their may may not be a resource behind this profile. Therefore we
        //  eat any not found exception called that a success. Remove when we enable authorization.
        try {
            iamService.deleteProfileResource(user, profileId);
        } catch (IamNotFoundException ex) {
            if (applicationConfiguration.isEnforceBillingProfileAuthorization()) {
                throw ex;
            }
            logger.warn("Ignoring billing profile not found exception", ex);
        }
    }

    // Verify user access to the billing account during billing profile creation
    public void verifyAccount(String billingAccountId, AuthenticatedUserRequest user) {
        if (!billingService.canAccess(user, billingAccountId)) {
            throw new InaccessibleBillingAccountException("The user '" + user.getEmail() +
                "' needs access to billing account '" + billingAccountId + "' to perform the requested operation");
        }
    }

    // -- profile upgrade --

    // Billing profiles prior to the introduction of this service did not have sam resources behind them.
    // This code generates sam resources for any billing profile it does not have one. It sets the
    // stewards group as the owner of the billing profile.
    //
    // In the current state, no billing profiles have sam resources, so the resources list will be empty.
    // When we upgrade a billing profile, we give stewards owner role. Since we run this
    // as a steward, we will be able to see any profiles we have already converted.
    //
    // The hole here is if someone gets in and creates a billing profile with a resource before
    // we run the upgrade. We would not retrieve that one here. However, Sam would not let us create
    // the resource in the step below. We would log an error, but otherwise, things would work.

    public UpgradeResponseModel upgradeProfileResources(UpgradeModel request, AuthenticatedUserRequest user) {

        Instant startTime = Instant.now();
        List<BillingProfileModel> profiles = profileDao.getOldBillingProfiles();

        List<UUID> resources = iamService.listAuthorizedResources(user, IamResourceType.SPEND_PROFILE);
        Set<String> profileResourceSet = resources.stream().map(r -> toString()).collect(Collectors.toSet());

        for (BillingProfileModel profile : profiles) {
            String profileId = profile.getId();
            if (!profileResourceSet.contains(profileId)) {
                try {
                    // No Sam profile, so let's make one - stewards are automatically added as owner
                    logger.info("Creating profile resource for {} ({})", profile.getProfileName(), profileId);
                    iamService.createProfileResource(user, profile.getId());
                } catch (Exception ex) {
                    logger.error("IAM failure during upgrade", ex);
                }
            }
        }

        return new UpgradeResponseModel()
            .upgradeName(request.getUpgradeName())
            .startTime(startTime.toString())
            .endTime(Instant.now().toString());
    }
}
