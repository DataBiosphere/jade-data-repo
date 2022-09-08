package bio.terra.service.auth.iam;

import static bio.terra.service.configuration.ConfigEnum.AUTH_CACHE_TIMEOUT_SECONDS;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.DatasetRequestModelPolicies;
import bio.terra.model.PolicyModel;
import bio.terra.model.SamPolicyModel;
import bio.terra.model.UserStatusInfo;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import bio.terra.service.auth.iam.exception.IamUnauthorizedException;
import bio.terra.service.auth.iam.exception.IamUnavailableException;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import java.io.IOException;
import java.time.Duration;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.broadinstitute.dsde.workbench.client.sam.model.UserStatus;
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
  private static final List<String> SAM_SCOPES = List.of("openid", "email", "profile");
  private static final Duration TOKEN_LENGTH = Duration.ofMinutes(5);

  private static final int AUTH_CACHE_TIMEOUT_SECONDS_DEFAULT = 60;

  private final IamProviderInterface iamProvider;
  private final ConfigurationService configurationService;
  private final Map<AuthorizedCacheKey, Boolean> authorizedMap;

  @Autowired
  public IamService(IamProviderInterface iamProvider, ConfigurationService configurationService) {
    this.iamProvider = iamProvider;
    this.configurationService = configurationService;
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
   * @throws IamUnauthorizedException if NOT authorized
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
   * Create a dataset IAM resource
   *
   * @param userReq authenticated user
   * @param datasetId id of the dataset
   * @param policies user emails to add as dataset policy members
   * @return List of policy group emails for the dataset policies
   */
  public Map<IamRole, String> createDatasetResource(
      AuthenticatedUserRequest userReq, UUID datasetId, DatasetRequestModelPolicies policies) {
    return callProvider(() -> iamProvider.createDatasetResource(userReq, datasetId, policies));
  }

  /**
   * Create a snapshot IAM resource
   *
   * @param userReq authenticated user
   * @param snapshotId id of the snapshot
   * @param readersList list of emails of users to add as readers of the snapshot
   * @return Policy group map
   */
  public Map<IamRole, String> createSnapshotResource(
      AuthenticatedUserRequest userReq, UUID snapshotId, List<String> readersList) {
    return callProvider(() -> iamProvider.createSnapshotResource(userReq, snapshotId, readersList));
  }

  // -- billing profile resource support --

  public void createProfileResource(AuthenticatedUserRequest userReq, String profileId) {
    callProvider(() -> iamProvider.createProfileResource(userReq, profileId));
  }

  public void deleteProfileResource(AuthenticatedUserRequest userReq, String profileId) {
    callProvider(() -> iamProvider.deleteProfileResource(userReq, profileId));
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

  public UserStatus registerUser(String serviceAccountEmail) {
    logger.info("Registering user %s in Terra".formatted(serviceAccountEmail));
    ImpersonatedCredentials impersonatedCredentials;
    try {
      impersonatedCredentials =
          ImpersonatedCredentials.create(
              GoogleCredentials.getApplicationDefault(),
              serviceAccountEmail,
              null,
              SAM_SCOPES,
              (int) TOKEN_LENGTH.toSeconds());
    } catch (IOException e) {
      throw new GoogleResourceException("Could not generate Google credentials", e);
    }

    String accessToken;
    try {
      accessToken = impersonatedCredentials.refreshAccessToken().getTokenValue();
    } catch (IOException e) {
      throw new GoogleResourceException("Could not generate Google access token", e);
    }
    return callProvider(() -> iamProvider.registerUser(accessToken));
  }
}
