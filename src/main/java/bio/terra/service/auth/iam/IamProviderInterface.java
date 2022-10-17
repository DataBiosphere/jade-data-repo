package bio.terra.service.auth.iam;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DatasetRequestModelPolicies;
import bio.terra.model.PolicyModel;
import bio.terra.model.RepositoryStatusModelSystems;
import bio.terra.model.SamPolicyModel;
import bio.terra.model.SnapshotRequestModelPolicies;
import bio.terra.model.UserStatusInfo;
import bio.terra.service.auth.iam.exception.IamUnauthorizedException;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import org.broadinstitute.dsde.workbench.client.sam.model.UserStatus;

/**
 * This is the interface to IAM used in the main body of the repository code. Right now, the only
 * implementation of this service is SAM, but we expect another implementation to be needed as part
 * of the Framework work.
 */
public interface IamProviderInterface {

  /**
   * Is a user authorized to do an action on a resource.
   *
   * @return true if authorized, false otherwise
   */
  boolean isAuthorized(
      AuthenticatedUserRequest userReq,
      IamResourceType iamResourceType,
      String resourceId,
      IamAction action)
      throws InterruptedException;

  /**
   * This is a wrapper method around {@link #isAuthorized(AuthenticatedUserRequest, IamResourceType,
   * String, IamAction)} that throws an exception instead of returning false when the user is NOT
   * authorized to do the action on the resource.
   *
   * @throws IamUnauthorizedException if NOT authorized
   */
  default void verifyAuthorization(
      AuthenticatedUserRequest userReq,
      IamResourceType iamResourceType,
      String resourceId,
      IamAction action)
      throws InterruptedException {
    String userEmail = userReq.getEmail();
    if (!isAuthorized(userReq, iamResourceType, resourceId, action)) {
      throw new IamUnauthorizedException(
          "User '" + userEmail + "' does not have required action: " + action);
    }
  }

  /**
   * Return the ids of resources of type iamResourceType that the user has access to, along with the
   * roles the user has on the resource.
   *
   * @param userReq authenticated user
   * @param iamResourceType resource type; e.g. dataset
   * @return Map of ids in UUID form to set of roles
   */
  Map<UUID, Set<IamRole>> listAuthorizedResources(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType)
      throws InterruptedException;

  /**
   * @param userReq authenticated user
   * @param iamResourceType resource type
   * @param resourceId resource in question
   * @return the user's available actions on that resource
   */
  List<String> listActions(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType, String resourceId)
      throws InterruptedException;

  /**
   * If user has any action on a resource than we allow that user to list the resource, rather than
   * have a specific action for listing. That is the Sam convention.
   *
   * @param userReq authenticated user
   * @param iamResourceType resource type
   * @param resourceId resource in question
   * @return true if the user has any actions on that resource
   */
  boolean hasAnyActions(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType, String resourceId)
      throws InterruptedException;

  /**
   * Delete a dataset IAM resource
   *
   * @param userReq authenticated user
   * @param datasetId dataset to delete
   */
  void deleteDatasetResource(AuthenticatedUserRequest userReq, UUID datasetId)
      throws InterruptedException;

  /**
   * Delete a snapshot IAM resource
   *
   * @param userReq authenticated user
   * @param snapshotId snapshot to delete
   */
  void deleteSnapshotResource(AuthenticatedUserRequest userReq, UUID snapshotId)
      throws InterruptedException;

  /**
   * Create a dataset IAM resource
   *
   * @param userReq authenticated user
   * @param datasetId id of the dataset
   * @param policies user emails to add as dataset policy members
   * @return Map of policy group emails for the dataset policies
   */
  Map<IamRole, String> createDatasetResource(
      AuthenticatedUserRequest userReq, UUID datasetId, DatasetRequestModelPolicies policies)
      throws InterruptedException;

  /**
   * Create a snapshot IAM resource
   *
   * @param userReq authenticated user
   * @param snapshotId id of the snapshot
   * @param policies user emails to add as snapshot policy members
   * @return Map of policy group emails for the snapshot policies
   */
  Map<IamRole, String> createSnapshotResource(
      AuthenticatedUserRequest userReq, UUID snapshotId, SnapshotRequestModelPolicies policies)
      throws InterruptedException;

  // -- billing profile resource support --

  /**
   * Create a spend profile IAM resource
   *
   * @param userReq authenticated user
   * @param profileId id of the snapshot
   */
  void createProfileResource(AuthenticatedUserRequest userReq, String profileId)
      throws InterruptedException;

  /**
   * Delete a spend profile IAM resource
   *
   * @param userReq authenticated user
   * @param profileId spend profile to delete
   */
  void deleteProfileResource(AuthenticatedUserRequest userReq, String profileId)
      throws InterruptedException;

  // -- policy membership support --

  List<SamPolicyModel> retrievePolicies(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType, UUID resourceId)
      throws InterruptedException;

  List<String> retrieveUserRoles(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType, UUID resourceId)
      throws InterruptedException;

  Map<IamRole, String> retrievePolicyEmails(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType, UUID resourceId)
      throws InterruptedException;

  PolicyModel addPolicyMember(
      AuthenticatedUserRequest userReq,
      IamResourceType iamResourceType,
      UUID resourceId,
      String policyName,
      String userEmail)
      throws InterruptedException;

  PolicyModel deletePolicyMember(
      AuthenticatedUserRequest userReq,
      IamResourceType iamResourceType,
      UUID resourceId,
      String policyName,
      String userEmail)
      throws InterruptedException;

  /**
   * Return a Google access token for an arbitrary pet service account associated with the logged in
   * user
   *
   * @param userReq authenticated user
   * @param scopes List of scopes to request for the token
   * @return A string access token
   */
  String getPetToken(AuthenticatedUserRequest userReq, List<String> scopes)
      throws InterruptedException;

  UserStatusInfo getUserInfo(AuthenticatedUserRequest userReq);

  /**
   * Returns the proxy group of a user
   *
   * @param userReq authenticated user
   * @return The google proxy group of a given user
   */
  String getProxyGroup(AuthenticatedUserRequest userReq) throws InterruptedException;

  /**
   * Register a user in Sam and make it usable (e.g. accept the ToS) using the specified access
   * token
   *
   * @param accessToken valid oauth token for user that is being registered in Terra
   * @return the fully registered user
   */
  UserStatus registerUser(String accessToken) throws InterruptedException;

  /**
   * Get Sam Status
   *
   * @return RepositoryStatusModelSystems model that includes status and message about sub-system
   *     statuses
   */
  RepositoryStatusModelSystems samStatus();

  /**
   * @param accessToken valid oauth token for the account fetching the group email
   * @param groupName name of Firecloud managed group
   * @return the group's email address
   */
  String getGroupEmail(String accessToken, String groupName) throws InterruptedException;

  /**
   * @param accessToken valid oauth token for the account creating the group
   * @param groupName name of Firecloud managed group to create
   */
  void createGroup(String accessToken, String groupName) throws InterruptedException;
}
