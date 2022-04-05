package bio.terra.service.auth.iam;

import static bio.terra.service.configuration.ConfigEnum.AUTH_CACHE_SIZE;
import static bio.terra.service.configuration.ConfigEnum.AUTH_CACHE_TIMEOUT_SECONDS;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.PolicyModel;
import bio.terra.model.UserStatusInfo;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import bio.terra.service.auth.iam.exception.IamUnauthorizedException;
import bio.terra.service.auth.iam.exception.IamUnavailableException;
import bio.terra.service.configuration.ConfigurationService;
import java.time.Instant;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;
import org.apache.commons.collections4.map.LRUMap;
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

  private static final int AUTH_CACHE_SIZE_DEFAULT = 100;

  private final IamProviderInterface iamProvider;
  private final ConfigurationService configurationService;
  private final Map<AuthorizedCacheKey, AuthorizedCacheValue> authorizedMap;

  @Autowired
  public IamService(IamProviderInterface iamProvider, ConfigurationService configurationService) {
    this.iamProvider = iamProvider;
    this.configurationService = configurationService;
    int cacheSize =
        Objects.requireNonNullElse(
            configurationService.getParameterValue(AUTH_CACHE_SIZE), AUTH_CACHE_SIZE_DEFAULT);
    // wrap the cache map with a synchronized map to safely share the cache across threads
    authorizedMap = Collections.synchronizedMap(new LRUMap<>(cacheSize));
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
   * Is a user authorized to do an action on a resource.
   *
   * @return true if authorized, false otherwise
   */
  public boolean isAuthorized(
      AuthenticatedUserRequest userReq,
      IamResourceType iamResourceType,
      String resourceId,
      IamAction action) {
    return callProvider(
        () -> {
          int timeoutSeconds = configurationService.getParameterValue(AUTH_CACHE_TIMEOUT_SECONDS);
          AuthorizedCacheKey authorizedCacheKey =
              new AuthorizedCacheKey(userReq, iamResourceType, resourceId, action);
          AuthorizedCacheValue authorizedCacheValue = authorizedMap.get(authorizedCacheKey);
          if (authorizedCacheValue != null) { // check if it's in the cache
            // check if it's still in the allotted time
            if (Instant.now().isBefore(authorizedCacheValue.getTimeout())) {
              logger.debug("Using the cache!");
              return authorizedCacheValue.isAuthorized();
            }
            authorizedMap.remove(authorizedCacheKey); // if timed out, remove it
          }
          boolean authorizedLookup =
              iamProvider.isAuthorized(userReq, iamResourceType, resourceId, action);
          Instant newTimeout = Instant.now().plusSeconds(timeoutSeconds);
          AuthorizedCacheValue newAuthorizedCacheValue =
              new AuthorizedCacheValue(newTimeout, authorizedLookup);
          authorizedMap.put(authorizedCacheKey, newAuthorizedCacheValue);
          // finally return the authorization
          return authorizedLookup;
        });
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
   * If user has any action on a resource than we allow that user to list the resource, rather than
   * have a specific action for listing. That is the Sam convention.
   *
   * @param userReq authenticated user
   * @param iamResourceType resource type
   * @param resourceId resource in question
   * @return true if the user has any actions on that resource
   */
  public boolean hasActions(
      AuthenticatedUserRequest userReq, IamResourceType iamResourceType, String resourceId) {
    return callProvider(() -> iamProvider.hasActions(userReq, iamResourceType, resourceId));
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
   * @return List of policy group emails for the dataset policies
   */
  public Map<IamRole, String> createDatasetResource(
      AuthenticatedUserRequest userReq, UUID datasetId) {
    return callProvider(() -> iamProvider.createDatasetResource(userReq, datasetId));
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

  public List<PolicyModel> retrievePolicies(
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
}
