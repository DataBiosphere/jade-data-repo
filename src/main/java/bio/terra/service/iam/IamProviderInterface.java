package bio.terra.service.iam;

import bio.terra.model.PolicyModel;
import bio.terra.model.UserStatusInfo;
import bio.terra.model.RepositoryStatusModelSystems;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.service.iam.exception.IamUnauthorizedException;

import java.util.List;
import java.util.Map;
import java.util.UUID;

/**
 * This is the interface to IAM used in the main body of the repository code.
 * Right now, the only implementation of this service is SAM, but we expect
 * another implementation to be needed as part of the Framework work.
 */
public interface IamProviderInterface {

    /**
     * Is a user authorized to do an action on a resource.
     *
     * @return true if authorized, false otherwise
     */
    boolean isAuthorized(AuthenticatedUserRequest userReq,
                         IamResourceType iamResourceType,
                         String resourceId,
                         IamAction action) throws InterruptedException;


    /**
     * This is a wrapper method around
     * {@link #isAuthorized(AuthenticatedUserRequest, IamResourceType, String, IamAction)} that throws
     * an exception instead of returning false when the user is NOT authorized to do the action on the resource.
     *
     * @throws IamUnauthorizedException if NOT authorized
     */
    default void verifyAuthorization(AuthenticatedUserRequest userReq,
                                     IamResourceType iamResourceType,
                                     String resourceId,
                                     IamAction action) throws InterruptedException {
        String userEmail = userReq.getEmail();
        if (!isAuthorized(userReq, iamResourceType, resourceId, action)) {
            throw new IamUnauthorizedException("User '" + userEmail + "' does not have required action: " + action);
        }
    }

    /**
     * List of the ids of the resources of iamResourceType that the user has any access to.
     *
     * @param userReq         authenticated user
     * @param iamResourceType resource type; e.g. dataset
     * @return List of ids in UUID form
     */
    List<UUID> listAuthorizedResources(AuthenticatedUserRequest userReq, IamResourceType iamResourceType)
        throws InterruptedException;

    /**
     * If user has any action on a resource than we allow that user to list the resource,
     * rather than have a specific action for listing. That is the Sam convention.
     *
     * @param userReq authenticated user
     * @param iamResourceType resource type
     * @param resourceId resource in question
     * @return true if the user has any actions on that resource
     */
    boolean hasActions(AuthenticatedUserRequest userReq,
                              IamResourceType iamResourceType,
                              String resourceId) throws InterruptedException;

        /**
         * Delete a dataset IAM resource
         *
         * @param userReq   authenticated user
         * @param datasetId dataset to delete
         */
    void deleteDatasetResource(AuthenticatedUserRequest userReq, UUID datasetId) throws InterruptedException;

    /**
     * Delete a snapshot IAM resource
     *
     * @param userReq    authenticated user
     * @param snapshotId snapshot to delete
     */
    void deleteSnapshotResource(AuthenticatedUserRequest userReq, UUID snapshotId) throws InterruptedException;

    /**
     * Create a dataset IAM resource
     *
     * @param userReq   authenticated user
     * @param datasetId id of the dataset
     * @return Map of policy group emails for the dataset policies
     */
    Map<IamRole, String> createDatasetResource(AuthenticatedUserRequest userReq, UUID datasetId)
        throws InterruptedException;

    /**
     * Create a snapshot IAM resource
     *
     * @param userReq     authenticated user
     * @param snapshotId  id of the snapshot
     * @param readersList list of emails of users to add as readers of the snapshot
     * @return Policy group email for the snapshot reader policy
     */
    Map<IamRole, String> createSnapshotResource(AuthenticatedUserRequest userReq,
                                                UUID snapshotId,
                                                List<String> readersList) throws InterruptedException;

    // -- billing profile resource support --

    /**
     * Create a spend profile IAM resource
     *
     * @param userReq   authenticated user
     * @param profileId id of the snapshot
     */
    void createProfileResource(AuthenticatedUserRequest userReq, String profileId) throws InterruptedException;

    /**
     * Update a spend profile IAM resource
     *
     * @param userReq   authenticated user
     * @param billingProfileRequestModel updated billing profile
     */
    void updateProfileResource(AuthenticatedUserRequest userReq, BillingProfileRequestModel billingProfileRequestModel)
        throws InterruptedException;

    /**
     * Delete a spend profile IAM resource
     *
     * @param userReq   authenticated user
     * @param profileId spend profile to delete
     */
    void deleteProfileResource(AuthenticatedUserRequest userReq, String profileId) throws InterruptedException;

    // -- policy membership support --

    List<PolicyModel> retrievePolicies(AuthenticatedUserRequest userReq,
                                       IamResourceType iamResourceType,
                                       UUID resourceId) throws InterruptedException;

    Map<IamRole, String> retrievePolicyEmails(AuthenticatedUserRequest userReq,
                                              IamResourceType iamResourceType,
                                              UUID resourceId) throws InterruptedException;

    PolicyModel addPolicyMember(AuthenticatedUserRequest userReq,
                                IamResourceType iamResourceType,
                                UUID resourceId,
                                String policyName,
                                String userEmail) throws InterruptedException;

    PolicyModel deletePolicyMember(AuthenticatedUserRequest userReq,
                                   IamResourceType iamResourceType,
                                   UUID resourceId,
                                   String policyName,
                                   String userEmail) throws InterruptedException;

    UserStatusInfo getUserInfo(AuthenticatedUserRequest userReq);

    /**
     * Get Sam Status
     *
     * @return RepositoryStatusModelSystems model that includes status and message about sub-system statuses
     */
    RepositoryStatusModelSystems samStatus();

}
