package bio.terra.service.profile;

import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.EnumerateBillingProfileModel;
import bio.terra.model.PolicyMemberRequest;
import bio.terra.model.PolicyModel;
import bio.terra.service.iam.AuthenticatedUserRequest;
import bio.terra.service.iam.IamAction;
import bio.terra.service.iam.IamResourceType;
import bio.terra.service.iam.IamService;
import bio.terra.service.iam.exception.IamUnauthorizedException;
import bio.terra.service.job.JobService;
import bio.terra.service.profile.exception.ProfileNotFoundException;
import bio.terra.service.profile.flight.ProfileMapKeys;
import bio.terra.service.profile.flight.create.ProfileCreateFlight;
import bio.terra.service.profile.flight.delete.ProfileDeleteFlight;
import bio.terra.service.profile.google.GoogleBillingService;
import bio.terra.service.resourcemanagement.exception.InaccessibleBillingAccountException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
public class ProfileService {

    private final ProfileDao profileDao;
    private final IamService iamService;
    private final JobService jobService;
    private final GoogleBillingService billingService;

    @Autowired
    public ProfileService(ProfileDao profileDao,
                          IamService iamService,
                          JobService jobService,
                          GoogleBillingService billingService) {
        this.profileDao = profileDao;
        this.iamService = iamService;
        this.jobService = jobService;
        this.billingService = billingService;
    }

    /**
     * Create a new billing profile providing an valid google billing account
     * We make the following checks:
     * <ul>
     *     <le>The service must have proper permissions on the google billing account</le>
     *     <le>The caller must have billing.resourceAssociation.create permission on the google billing account</le>
     *     <le>The google billing account must be enabled</le>
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
     * Remove billing profile. We make the following checks:
     * <ul>
     *     <le>the caller must be an owner of the billing profile</le>
     *     <le>There must be no dependencies on the billing profile;
     *     that is, no snapshots, dataset, or buckets referencing the profile</le>
     * </ul>
     *
     * @param id   the unique id of the bill profile
     * @param user the user attempting the delete
     * @return jobId of the submitted stairway job
     */
    public String deleteProfile(String id, AuthenticatedUserRequest user) {
        //TODO ADD BACK
        //iamService.verifyAuthorization(user, IamResourceType.SPEND_PROFILE, id, IamAction.DELETE);

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
     * @param limit  maximum number of profiles to return in this request
     * @param user   user on whose behalf we are making this request
     * @return enumeration profile containing the list and total
     */
    public EnumerateBillingProfileModel enumerateProfiles(Integer offset,
                                                          Integer limit,
                                                          AuthenticatedUserRequest user) {
        //TODO add back
        //List<UUID> resources = iamService.listAuthorizedResources(user, IamResourceType.SPEND_PROFILE);
        //if (resources.isEmpty()) {
        return new EnumerateBillingProfileModel().total(0);
        //}
        //return profileDao.enumerateBillingProfiles(offset, limit, resources);
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
        //TODO add back
        /*if (!iamService.hasActions(user, IamResourceType.SPEND_PROFILE, id)) {
            throw new IamUnauthorizedException("unauthorized");
        }*/

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
        // TODO: Add this back in once we have way to authorize w/ existing billing profiles
        /*iamService.verifyAuthorization(user,
            IamResourceType.SPEND_PROFILE,
            profileId.toString(),
            IamAction.LINK);*/
        BillingProfileModel profileModel = profileDao.getBillingProfileById(profileId);

        // TODO: check bill account usable and validate delegation path
        //  For now we just make sure that the building account is accessible to the
        //  TDR service account.
        String billingAccountId = profileModel.getBillingAccountId();
        if (!billingService.canAccess(billingAccountId)) {
            throw new InaccessibleBillingAccountException("The repository needs access to billing account "
                + billingAccountId + " to perform the requested operation");
        }

        return profileModel;
    }

    public PolicyModel addProfilePolicyMember(String profileId,
                                              String policyName,
                                              PolicyMemberRequest policyMember,
                                              AuthenticatedUserRequest user) {
        return new PolicyModel();/*iamService.addPolicyMember(
            user,
            IamResourceType.SPEND_PROFILE,
            UUID.fromString(profileId),
            policyName,
            policyMember.getEmail());*/
    }

    // -- methods invoked from billing profile flights --

    public BillingProfileModel createProfileMetadata(BillingProfileRequestModel profileRequest,
                                                     AuthenticatedUserRequest user) {
        return profileDao.createBillingProfile(profileRequest, user.getEmail());
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
        iamService.deleteProfileResource(user, profileId);
    }

    // Verify access to the billing account during billing profile creation
    public void verifyAccount(BillingProfileRequestModel request, AuthenticatedUserRequest user) {
        // TODO: check bill account usable and creator has link access
        //  For now we just make sure that the billing account is accessible to the
        //  TDR service account.
        String billingAccountId = request.getBillingAccountId();
        if (!billingService.canAccess(billingAccountId)) {
            throw new InaccessibleBillingAccountException("The repository needs access to billing account "
                + billingAccountId + " to perform the requested operation");
        }
    }

}
