package bio.terra.service.auth.iam;

import static bio.terra.service.configuration.ConfigEnum.AUTH_CACHE_TIMEOUT_SECONDS;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.PolicyModel;
import bio.terra.model.SamPolicyModel;
import bio.terra.model.SnapshotRequestModel;
import bio.terra.model.SnapshotRequestModelPolicies;
import bio.terra.model.UserStatusInfo;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import bio.terra.service.auth.iam.exception.IamUnavailableException;
import bio.terra.service.auth.oauth2.GoogleCredentialsService;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.journal.JournalService;
import com.google.auth.oauth2.ImpersonatedCredentials;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections4.ListUtils;
import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

/**
 * The IamProvider code is used both in flights and from the REST API. It needs to be able to throw
 * InterruptedException to be caught by Stairway as part of shutdown processing.
 *
 * <p>In the REST API controller, we cannot just specify `throws InterruptedException` (or any
 * checked exception), because the controller derives from the swagger-codegen interface definition.
 * That definition does not allow for any checked exceptions.
 *
 * <p>This IamService is a thin layer that calls the IamProviderInterface, but catches
 * InterruptedExceptions and converts them into a RuntimeException: IamUnavailableException. That
 * throw will get processed by the global exception handler and make the right error return to the
 * caller.
 */
@Component
public class IamService {
  private final Logger logger = LoggerFactory.getLogger(IamService.class);
  private static final List<String> SCOPES = List.of("openid", "email", "profile");
  private static final Duration TOKEN_LENGTH = Duration.ofMinutes(5);

  private static final int AUTH_CACHE_TIMEOUT_SECONDS_DEFAULT = 60;

  private final IamProviderInterface iamProvider;
  private final Map<AuthorizedCacheKey, Boolean> authorizedMap;
  private final JournalService journalService;
  private final GoogleCredentialsService googleCredentialsService;

  @Autowired
  public IamService(
      IamProviderInterface iamProvider,
      ConfigurationService configurationService,
      JournalService journalService,
      GoogleCredentialsService googleCredentialsService) {
    this.iamProvider = iamProvider;
    this.journalService = journalService;
    this.googleCredentialsService = googleCredentialsService;
    int ttl =
        Objects.requireNonNullElse(
            configurationService.getParameterValue(AUTH_CACHE_TIMEOUT_SECONDS),
            AUTH_CACHE_TIMEOUT_SECONDS_DEFAULT);
    // wrap the cache map with a synchronized map to safely share the cache across threads
    authorizedMap = Collections.synchronizedMap(new PassiveExpiringMap<>(ttl, TimeUnit.SECONDS));
  }

  private interface Call<T> {
    T get() throws InterruptedException;
  }

  private interface VoidCall {
    void get() throws InterruptedException;
  }

  private <T> T callProvider(Call<T> call) {
    try {
      return call.get();
    } catch (InterruptedException e) {
      throw new IamUnavailableException("service unavailable", e);
    }
  }

  private void callProvider(VoidCall call) {
    callProvider(
        () -> {
          call.get();
          return null;
        });
  }

  /**
   * Check a cache to determine whether a user is authorized to do an action on a resource.
   *
   * @return true if authorized, false otherwise
   */
  public boolean isAuthorized(
      AuthenticatedUserRequest userReq,
      IamResourceType iamResourceType,
      String resourceId,
      IamAction action) {
    return authorizedMap.computeIfAbsent(
        new AuthorizedCacheKey(userReq, iamResourceType, resourceId, action),
        this::computeAuthorized);
  }

  /**
   * Call external API to determine whether a user is authorized to do an action on a resource.
   *
   * @return true if authorized, false otherwise
   */
  private boolean computeAuthorized(AuthorizedCacheKey key) {
    return callProvider(
        () ->
            iamProvider.isAuthorized(
                key.userReq(), key.iamResourceType(), key.resourceId(), key.action()));
  }

  /**
   * This is a wrapper method around {@link #isAuthorized(AuthenticatedUserRequest, IamResourceType,
   * String, IamAction)} that throws an exception instead of returning false when the user is NOT
   * authorized to do the action on the resource.
   *
   * @throws IamForbiddenException if NOT authorized
   */
  public void verifyAuthorization(
      AuthenticatedUserRequest userReq,
      IamResourceType iamResourceType,
      String resourceId,
      IamAction action) {
    String userEmail = userReq.getEmail();
    if (!isAuthorized(userReq, iamResourceType, resourceId, action)) {
      throw new IamForbiddenException(
          "User '" + userEmail + "' does not have required action: " + action);
    }
  }

  /**
   * This is a wrapper method around {@link #hasAnyActions(AuthenticatedUserRequest,
   * IamResourceType, String)} that throws an exception instead of returning false when the user
   * holds no actions on the resource.
   *
   * @throws IamForbiddenException if NOT authorized to perform any action on the resource
   */
  public void verifyAuthorization(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType, String resourceId) {
    String userEmail = userReq.getEmail();
    if (!hasAnyActions(userReq, iamResourceType, resourceId)) {
      throw new IamForbiddenException(
          "User '" + userEmail + "' does not have any actions on the resource");
    }
  }

  /**
   * List of the ids of the resources of iamResourceType that the user has any access to.
   *
   * @param userReq authenticated user
   * @param iamResourceType resource type; e.g. dataset
   * @return List of ids in UUID form
   */
  public Map<UUID, Set<IamRole>> listAuthorizedResources(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType) {
    return callProvider(() -> iamProvider.listAuthorizedResources(userReq, iamResourceType));
  }

  /**
   * Note: calling this method will trigger a call to SAM API. No cache backs it. If calling,
   * consider whether new usage patterns necessitate refactoring to add a cache.
   *
   * @param userReq authenticated user
   * @param iamResourceType resource type; e.g. dataset
   * @param resourceId UUID identifying a resource
   * @param actions required
   * @throws IamForbiddenException if the user is missing any `actions` on the resource
   */
  public void verifyAuthorizations(
      AuthenticatedUserRequest userReq,
      IamResourceType iamResourceType,
      String resourceId,
      Collection<IamAction> actions)
      throws IamForbiddenException {
    String userEmail = userReq.getEmail();
    List<String> availableActions =
        callProvider(() -> iamProvider.listActions(userReq, iamResourceType, resourceId));

    List<String> unavailableActions =
        actions.stream()
            .map(IamAction::toString)
            .filter(action -> !availableActions.contains(action))
            .toList();

    if (!unavailableActions.isEmpty()) {
      throw new IamForbiddenException(
          "User '" + userEmail + "' is missing required actions (returned in details)",
          unavailableActions);
    }
  }

  /**
   * If user has any action on a resource than we allow that user to list the resource, rather than
   * have a specific action for listing. That is the Sam convention.
   *
   * @param userReq authenticated user
   * @param iamResourceType resource type
   * @param resourceId resource in question
   * @return true if the user has any actions on that resource
   */
  public boolean hasAnyActions(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType, String resourceId) {
    return callProvider(() -> iamProvider.hasAnyActions(userReq, iamResourceType, resourceId));
  }

  /**
   * Delete a dataset IAM resource
   *
   * @param userReq authenticated user
   * @param datasetId dataset to delete
   */
  public void deleteDatasetResource(AuthenticatedUserRequest userReq, UUID datasetId) {
    callProvider(() -> iamProvider.deleteDatasetResource(userReq, datasetId));
  }

  /**
   * Delete a snapshot IAM resource
   *
   * @param userReq authenticated user
   * @param snapshotId snapshot to delete
   */
  public void deleteSnapshotResource(AuthenticatedUserRequest userReq, UUID snapshotId) {
    callProvider(() -> iamProvider.deleteSnapshotResource(userReq, snapshotId));
  }

  /**
   * Create a snapshot IAM resource
   *
   * @param userReq authenticated user
   * @param snapshotId id of the snapshot
   * @param policies user emails to add as snapshot policy members
   * @return Map of policy group emails for the snapshot policies
   */
  public Map<IamRole, String> createSnapshotResource(
      AuthenticatedUserRequest userReq, UUID snapshotId, SnapshotRequestModelPolicies policies) {
    return callProvider(() -> iamProvider.createSnapshotResource(userReq, snapshotId, policies));
  }

  /**
   * Create a snapshot builder request IAM resource
   *
   * @param userReq authenticated user
   * @param snapshotId the snapshot id of the snapshot this is based off of
   * @param snapshotBuilderRequestId id of the snapshot request
   * @return Map of policy group emails for the snapshot builder request policies
   */
  public Map<IamRole, List<String>> createSnapshotBuilderRequestResource(
      AuthenticatedUserRequest userReq, UUID snapshotId, UUID snapshotBuilderRequestId) {
    return callProvider(
        () ->
            iamProvider.createSnapshotBuilderRequestResource(
                userReq, snapshotId, snapshotBuilderRequestId));
  }

  /**
   * @param request snapshot creation request
   * @return user-defined snapshot policy object, supplemented with readers from deprecated input
   */
  public SnapshotRequestModelPolicies deriveSnapshotPolicies(SnapshotRequestModel request) {
    SnapshotRequestModelPolicies policies =
        Optional.ofNullable(request.getPolicies()).orElseGet(SnapshotRequestModelPolicies::new);

    // While duplicate readers are possible in this combination, we do not need to deduplicate:
    // SAM handles duplicate policy members without issue.
    List<String> combinedReaders = new ArrayList<>();
    combinedReaders.addAll(ListUtils.emptyIfNull(policies.getReaders()));
    combinedReaders.addAll(ListUtils.emptyIfNull(request.getReaders()));

    return policies.readers(combinedReaders);
  }

  // -- billing profile resource support --

  public void createProfileResource(AuthenticatedUserRequest userReq, String profileId) {
    callProvider(() -> iamProvider.createProfileResource(userReq, profileId));
  }

  public void deleteProfileResource(AuthenticatedUserRequest userReq, String profileId) {
    callProvider(() -> iamProvider.deleteProfileResource(userReq, profileId));
  }

  // -- auth domain support --
  public List<String> retrieveAuthDomain(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType, UUID resourceId) {
    return callProvider(() -> iamProvider.retrieveAuthDomain(userReq, iamResourceType, resourceId));
  }

  public void patchAuthDomain(
      AuthenticatedUserRequest userReq,
      IamResourceType iamResourceType,
      UUID resourceId,
      List<String> userGroups) {
    callProvider(
        () -> iamProvider.patchAuthDomain(userReq, iamResourceType, resourceId, userGroups));
  }

  // -- policy membership support --

  public List<SamPolicyModel> retrievePolicies(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType, UUID resourceId) {
    return callProvider(() -> iamProvider.retrievePolicies(userReq, iamResourceType, resourceId));
  }

  public Map<IamRole, String> retrievePolicyEmails(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType, UUID resourceId) {
    return callProvider(
        () -> iamProvider.retrievePolicyEmails(userReq, iamResourceType, resourceId));
  }

  public PolicyModel addPolicyMember(
      AuthenticatedUserRequest userReq,
      IamResourceType iamResourceType,
      UUID resourceId,
      String policyName,
      String userEmail) {
    return callProvider(
        () -> {
          PolicyModel policy =
              iamProvider.addPolicyMember(
                  userReq, iamResourceType, resourceId, policyName, userEmail);
          // Invalidate the cache
          authorizedMap.clear();
          journalService.recordUpdate(
              userReq,
              resourceId,
              iamResourceType,
              String.format("Added %s to %s", userEmail, policyName),
              null);
          return policy;
        });
  }

  public PolicyModel deletePolicyMember(
      AuthenticatedUserRequest userReq,
      IamResourceType iamResourceType,
      UUID resourceId,
      String policyName,
      String userEmail) {
    return callProvider(
        () -> {
          PolicyModel policy =
              iamProvider.deletePolicyMember(
                  userReq, iamResourceType, resourceId, policyName, userEmail);
          // Invalidate the cache
          authorizedMap.clear();
          journalService.recordUpdate(
              userReq,
              resourceId,
              iamResourceType,
              String.format("Removed %s from %s", userEmail, policyName),
              null);
          return policy;
        });
  }

  public List<String> retrieveUserRoles(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType, UUID resourceId) {
    return callProvider(() -> iamProvider.retrieveUserRoles(userReq, iamResourceType, resourceId));
  }

  public UserStatusInfo getUserInfo(AuthenticatedUserRequest userReq) {
    return iamProvider.getUserInfo(userReq);
  }

  public void registerUser(String serviceAccountEmail) {
    logger.info("Registering user %s in Terra".formatted(serviceAccountEmail));
    ImpersonatedCredentials impersonatedCredentials =
        ImpersonatedCredentials.create(
            googleCredentialsService.getApplicationDefault(),
            serviceAccountEmail,
            null,
            SCOPES,
            (int) TOKEN_LENGTH.toSeconds());
    String accessToken = googleCredentialsService.getAccessToken(impersonatedCredentials, SCOPES);
    callProvider(() -> iamProvider.registerUser(accessToken));
  }

  // -- managed group support --

  /**
   * @param groupName Firecloud managed group to create as the TDR SA
   * @return the email for the newly created group
   */
  public String createGroup(String groupName) {
    String tdrSaAccessToken = googleCredentialsService.getApplicationDefaultAccessToken(SCOPES);
    return callProvider(() -> iamProvider.createGroup(tdrSaAccessToken, groupName));
  }

  /**
   * @param groupName Firecloud managed group
   * @param policyName name of Firecloud managed group policy
   * @param emailAddresses emails which the TDR SA will set as group policy members
   */
  public void overwriteGroupPolicyEmails(
      String groupName, String policyName, List<String> emailAddresses) {
    String tdrSaAccessToken = googleCredentialsService.getApplicationDefaultAccessToken(SCOPES);
    callProvider(
        () ->
            iamProvider.overwriteGroupPolicyEmails(
                tdrSaAccessToken, groupName, policyName, emailAddresses));
  }

  /**
   * @param groupName Firecloud managed group to delete as the TDR SA
   */
  public void deleteGroup(String groupName) {
    String tdrSaAccessToken = googleCredentialsService.getApplicationDefaultAccessToken(SCOPES);
    callProvider(() -> iamProvider.deleteGroup(tdrSaAccessToken, groupName));
  }

  /**
   * Gets a signed URL for the given blob, signed by the Pet Service account of the calling user.
   * The signed URL is scoped to the permissions of the signing Pet Service Account. Will provide a
   * signed URL for any object path, even if that object does not exist. The signed URL will work
   * for data stored in requester pays hosted buckets.
   *
   * @param userReq authenticated user
   * @param project Google project to use to sign blob
   * @param path path to blob to sign
   * @param duration duration of the signed URL
   * @return signed URL containing the project as well as the email address of the user who
   *     requested the URL for auditing purposes
   */
  public String signUrlForBlob(
      AuthenticatedUserRequest userReq, String project, String path, Duration duration) {
    return callProvider(() -> iamProvider.signUrlForBlob(userReq, project, path, duration));
  }
}
