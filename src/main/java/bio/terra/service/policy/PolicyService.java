package bio.terra.service.policy;

import bio.terra.app.configuration.PolicyServiceConfiguration;
import bio.terra.common.ExceptionUtils;
import bio.terra.model.RepositoryStatusModelSystems;
import bio.terra.policy.api.PublicApi;
import bio.terra.policy.api.TpsApi;
import bio.terra.policy.client.ApiException;
import bio.terra.policy.model.TpsComponent;
import bio.terra.policy.model.TpsObjectType;
import bio.terra.policy.model.TpsPaoCreateRequest;
import bio.terra.policy.model.TpsPolicyInput;
import bio.terra.policy.model.TpsPolicyInputs;
import bio.terra.service.policy.exception.PolicyConflictException;
import bio.terra.service.policy.exception.PolicyServiceApiException;
import bio.terra.service.policy.exception.PolicyServiceAuthorizationException;
import bio.terra.service.policy.exception.PolicyServiceDuplicateException;
import bio.terra.service.policy.exception.PolicyServiceNotFoundException;
import com.google.common.annotations.VisibleForTesting;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class PolicyService {
  public static final String POLICY_NAMESPACE = "terra";
  public static final String PROTECTED_DATA_POLICY_NAME = "protected-data";
  private static final Logger logger = LoggerFactory.getLogger(PolicyService.class);
  private final PolicyServiceConfiguration policyServiceConfiguration;
  private final PolicyApiService policyApiService;

  public PolicyService(
      PolicyServiceConfiguration policyServiceConfiguration, PolicyApiService policyApiService) {
    this.policyServiceConfiguration = policyServiceConfiguration;
    this.policyApiService = policyApiService;
  }

  // -- Policy Attribute Object Interface --
  public static TpsPolicyInput getProtectedDataPolicyInput() {
    return new TpsPolicyInput().namespace(POLICY_NAMESPACE).name(PROTECTED_DATA_POLICY_NAME);
  }

  public void createSnapshotPao(UUID snapshotId, @Nullable TpsPolicyInputs policyInputs) {
    createPao(snapshotId, TpsObjectType.SNAPSHOT, policyInputs);
  }

  public void createPao(
      UUID resourceId, TpsObjectType resourceType, @Nullable TpsPolicyInputs policyInputs) {
    policyServiceConfiguration.tpsEnabledCheck();
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

  public void deletePao(UUID resourceId) {
    policyServiceConfiguration.tpsEnabledCheck();
    TpsApi tpsApi = policyApiService.getPolicyApi();
    try {
      tpsApi.deletePao(resourceId);
    } catch (ApiException e) {
      RuntimeException exception = convertApiException(e);
      if (PolicyServiceNotFoundException.class.isAssignableFrom(exception.getClass())) {
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
      return new RepositoryStatusModelSystems()
          .ok(true)
          .critical(policyServiceConfiguration.getEnabled())
          .message("Terra Policy Service status ok");
    } catch (Exception ex) {
      String errorMsg = "Terra Policy Service status check failed";
      logger.error(errorMsg, ex);
      return new RepositoryStatusModelSystems()
          .ok(false)
          .critical(policyServiceConfiguration.getEnabled())
          .message(errorMsg + ": " + ExceptionUtils.formatException(ex));
    }
  }
}
