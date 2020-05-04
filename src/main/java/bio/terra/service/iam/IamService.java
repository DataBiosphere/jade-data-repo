package bio.terra.service.iam;

import bio.terra.model.PolicyModel;
import bio.terra.model.UserStatusInfo;
import bio.terra.service.iam.exception.IamUnauthorizedException;
import bio.terra.service.iam.exception.IamUnavailableException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.UUID;

@Component
public class IamService {
    private final IamProviderInterface iamProvider;

    @Autowired
    public IamService(IamProviderInterface iamProvider) {
        this.iamProvider = iamProvider;
    }

    /**
     * Is a user authorized to do an action on a resource.
     * @return true if authorized, false otherwise
     */
    public boolean isAuthorized(AuthenticatedUserRequest userReq,
                         IamResourceType iamResourceType,
                         String resourceId,
                         IamAction action) {
        try {
            return iamProvider.isAuthorized(userReq, iamResourceType, resourceId, action);
        } catch (InterruptedException ex) {
            throw new IamUnavailableException("service unavailable");
        }
    }

    /**
     * This is a wrapper method around
     * {@link #isAuthorized(AuthenticatedUserRequest, IamResourceType, String, IamAction)} that throws
     * an exception instead of returning false when the user is NOT authorized to do the action on the resource.
     * @throws IamUnauthorizedException if NOT authorized
     */
    public void verifyAuthorization(AuthenticatedUserRequest userReq,
                                    IamResourceType iamResourceType,
                                    String resourceId,
                                    IamAction action) {
        String userEmail = userReq.getEmail();
        if (!isAuthorized(userReq, iamResourceType, resourceId, action)) {
            throw new IamUnauthorizedException("User '" + userEmail + "' does not have required action: " + action);
        }
    }

    /**
     * List of the ids of the resources of iamResourceType that the user has any access to.
     * @param userReq authenticated user
     * @param iamResourceType resource type; e.g. dataset
     * @return List of ids in UUID form
     */
    public List<UUID> listAuthorizedResources(AuthenticatedUserRequest userReq, IamResourceType iamResourceType) {
        try {
            return iamProvider.listAuthorizedResources(userReq, iamResourceType);
        } catch (InterruptedException ex) {
            throw new IamUnavailableException("service unavailable");
        }
    }

    /**
     * Delete a dataset IAM resource
     * @param userReq authenticated user
     * @param datasetId dataset to delete
     */
    public void deleteDatasetResource(AuthenticatedUserRequest userReq, UUID datasetId) {
        try {
            iamProvider.deleteDatasetResource(userReq, datasetId);
        } catch (InterruptedException ex) {
            throw new IamUnavailableException("service unavailable");
        }
    }

    /**
     * Delete a snapshot IAM resource
     * @param userReq authenticated user
     * @param snapshotId snapshot to delete
     */
    public void deleteSnapshotResource(AuthenticatedUserRequest userReq, UUID snapshotId) {
        try {
            iamProvider.deleteSnapshotResource(userReq, snapshotId);
        } catch (InterruptedException ex) {
            throw new IamUnavailableException("service unavailable");
        }
    }

    /**
     * Create a dataset IAM resource
     *
     * @param userReq authenticated user
     * @param datasetId id of the dataset
     * @return List of policy group emails for the dataset policies
     */
    public List<String> createDatasetResource(AuthenticatedUserRequest userReq, UUID datasetId) {
        try {
            return iamProvider.createDatasetResource(userReq, datasetId);
        } catch (InterruptedException ex) {
            throw new IamUnavailableException("service unavailable");
        }
    }

    /**
     * Create a snapshot IAM resource
     *
     * @param userReq authenticated user
     * @param snapshotId id of the snapshot
     * @param readersList list of emails of users to add as readers of the snapshot
     * @return Policy group email for the snapshot reader policy
     */
    public String createSnapshotResource(AuthenticatedUserRequest userReq, UUID snapshotId, List<String> readersList) {
        try {
            return iamProvider.createSnapshotResource(userReq, snapshotId, readersList);
        } catch (InterruptedException ex) {
            throw new IamUnavailableException("service unavailable");
        }
    }

    // -- policy membership support --

    public List<PolicyModel> retrievePolicies(AuthenticatedUserRequest userReq,
                                              IamResourceType iamResourceType,
                                              UUID resourceId) {
        try {
            return iamProvider.retrievePolicies(userReq, iamResourceType, resourceId);
        } catch (InterruptedException ex) {
            throw new IamUnavailableException("service unavailable");
        }
    }

    public PolicyModel addPolicyMember(AuthenticatedUserRequest userReq,
                                IamResourceType iamResourceType,
                                UUID resourceId,
                                String policyName,
                                String userEmail) {
        try {
            return iamProvider.addPolicyMember(userReq, iamResourceType, resourceId, policyName, userEmail);
        } catch (InterruptedException ex) {
            throw new IamUnavailableException("service unavailable");
        }
    }

    public PolicyModel deletePolicyMember(AuthenticatedUserRequest userReq,
                                   IamResourceType iamResourceType,
                                   UUID resourceId,
                                   String policyName,
                                   String userEmail) {
        try {
            return iamProvider.deletePolicyMember(userReq, iamResourceType, resourceId, policyName, userEmail);
        } catch (InterruptedException ex) {
            throw new IamUnavailableException("service unavailable");
        }
    }

    public UserStatusInfo getUserInfo(AuthenticatedUserRequest userReq) {
        return iamProvider.getUserInfo(userReq);
    }
}
