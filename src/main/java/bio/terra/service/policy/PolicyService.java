package bio.terra.service.policy;

import bio.terra.common.ExceptionUtils;
import bio.terra.model.RepositoryStatusModelSystems;
import bio.terra.policy.api.PublicApi;
import bio.terra.policy.api.TpsApi;
import bio.terra.policy.client.ApiException;
import bio.terra.policy.model.TpsComponent;
import bio.terra.policy.model.TpsObjectType;
import bio.terra.policy.model.TpsPaoCreateRequest;
import bio.terra.policy.model.TpsPaoUpdateRequest;
import bio.terra.policy.model.TpsPolicyInput;
import bio.terra.policy.model.TpsPolicyInputs;
import bio.terra.policy.model.TpsPolicyPair;
import bio.terra.policy.model.TpsUpdateMode;
import bio.terra.service.policy.exception.PolicyConflictException;
import bio.terra.service.policy.exception.PolicyServiceApiException;
import bio.terra.service.policy.exception.PolicyServiceAuthorizationException;
import bio.terra.service.policy.exception.PolicyServiceDuplicateException;
import bio.terra.service.policy.exception.PolicyServiceNotFoundException;
import com.google.common.annotations.VisibleForTesting;
import jakarta.annotation.Nullable;
import java.util.UUID;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class PolicyService {
  public static final String POLICY_NAMESPACE = "terra";
  public static final String GROUP_CONSTRAINT_POLICY_NAME = "group-constraint";
  public static final String GROUP_CONSTRAINT_KEY_NAME = "group";
  public static final String PROTECTED_DATA_POLICY_NAME = "protected-data";
  public static final TpsUpdateMode UPDATE_MODE = TpsUpdateMode.FAIL_ON_CONFLICT;
  private static final Logger logger = LoggerFactory.getLogger(PolicyService.class);

  private final PolicyApiService policyApiService;

  public PolicyService(PolicyApiService policyApiService) {
    this.policyApiService = policyApiService;
  }

  // -- Policy Attribute Object Interface --

  public static TpsPolicyInput getGroupConstraintPolicyInput(String groupName) {
    TpsPolicyPair policyPair = new TpsPolicyPair().key(GROUP_CONSTRAINT_KEY_NAME).value(groupName);
    return new TpsPolicyInput()
        .namespace(POLICY_NAMESPACE)
        .name(GROUP_CONSTRAINT_POLICY_NAME)
        .addAdditionalDataItem(policyPair);
  }

  public static TpsPolicyInput getProtectedDataPolicyInput() {
    return new TpsPolicyInput().namespace(POLICY_NAMESPACE).name(PROTECTED_DATA_POLICY_NAME);
  }

  public void createSnapshotPao(UUID snapshotId, @Nullable TpsPolicyInputs policyInputs) {
    createPao(snapshotId, TpsObjectType.SNAPSHOT, policyInputs);
  }

  public void createPao(
      UUID resourceId, TpsObjectType resourceType, @Nullable TpsPolicyInputs policyInputs) {
    TpsPolicyInputs inputs = (policyInputs == null) ? new TpsPolicyInputs() : policyInputs;
    TpsApi tpsApi = policyApiService.getPolicyApi();
    try {
      tpsApi.createPao(
          new TpsPaoCreateRequest()
              .objectId(resourceId)
              .component(TpsComponent.TDR)
              .objectType(resourceType)
              .attributes(inputs));
    } catch (ApiException e) {
      throw convertApiException(e);
    }
  }

  public void updatePao(TpsPaoUpdateRequest updateRequest, UUID resourceId) {
    TpsApi tpsApi = policyApiService.getPolicyApi();
    // Setting the update mode is required, but not enforced by the terra-policy-client.
    updateRequest = updateRequest.updateMode(UPDATE_MODE);
    try {
      tpsApi.updatePao(updateRequest, resourceId);
    } catch (ApiException e) {
      throw convertApiException(e);
    }
  }

  public void createOrUpdatePao(
      UUID resourceId, TpsObjectType resourceType, @Nullable TpsPolicyInputs policyInputs) {
    try {
      createPao(resourceId, resourceType, policyInputs);
    } catch (PolicyConflictException | PolicyServiceDuplicateException e) {
      // TODO - handling PolicyServiceDuplicateException may be removed once
      // https://broadworkbench.atlassian.net/browse/ID-899
      // is complete, as this error should instead throw a PolicyConflictException.
      TpsPaoUpdateRequest updateRequest = new TpsPaoUpdateRequest().addAttributes(policyInputs);
      updatePao(updateRequest, resourceId);
    }
  }

  /**
   * Delete the policy access object by its id. If it does not exist in the policy service, ignore
   * the PolicyServiceNotFoundException exception.
   */
  public void deletePaoIfExists(UUID resourceId) {
    TpsApi tpsApi = policyApiService.getPolicyApi();
    try {
      tpsApi.deletePao(resourceId);
    } catch (ApiException e) {
      RuntimeException exception = convertApiException(e);
      if (!(exception instanceof PolicyServiceNotFoundException)) {
        throw exception;
      }
    }
  }

  @VisibleForTesting
  static RuntimeException convertApiException(ApiException ex) {
    return switch (ex.getCode()) {
      case HttpStatus.SC_UNAUTHORIZED -> new PolicyServiceAuthorizationException(
          "Not authorized to access Terra Policy Service", ex.getCause());
      case HttpStatus.SC_NOT_FOUND -> new PolicyServiceNotFoundException(
          "Policy access object not found", ex);
      case HttpStatus.SC_BAD_REQUEST -> {
        if (StringUtils.containsIgnoreCase(ex.getMessage(), "duplicate")) {
          // TODO - this special handling may be removed once
          // https://broadworkbench.atlassian.net/browse/ID-899
          // is complete, as this error should instead throw a 409.
          yield new PolicyServiceDuplicateException(
              "Request contains duplicate policy attribute", ex);
        } else {
          yield new PolicyServiceApiException(ex);
        }
      }
      case HttpStatus.SC_CONFLICT -> new PolicyConflictException(
          "Policy access object already exists", ex);
      default -> new PolicyServiceApiException(ex);
    };
  }

  /**
   * @return status of Terra Policy Service (client does not return subsystem info)
   */
  public RepositoryStatusModelSystems status() {
    PublicApi publicApi = policyApiService.getUnauthPolicyApi();
    try {
      publicApi.getStatus();
      return new RepositoryStatusModelSystems().ok(true).message("Terra Policy Service status ok");
    } catch (Exception ex) {
      String errorMsg = "Terra Policy Service status check failed";
      logger.error(errorMsg, ex);
      return new RepositoryStatusModelSystems()
          .ok(false)
          .message(errorMsg + ": " + ExceptionUtils.formatException(ex));
    }
  }
}
