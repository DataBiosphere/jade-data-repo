package bio.terra.service.policy;

import bio.terra.app.configuration.PolicyServiceConfiguration;
import bio.terra.common.logging.RequestIdFilter;
import bio.terra.policy.api.TpsApi;
import bio.terra.policy.client.ApiClient;
import bio.terra.policy.client.ApiException;
import bio.terra.policy.model.TpsComponent;
import bio.terra.policy.model.TpsObjectType;
import bio.terra.policy.model.TpsPaoCreateRequest;
import bio.terra.policy.model.TpsPolicyInputs;
import bio.terra.service.policy.exception.PolicyServiceAPIException;
import bio.terra.service.policy.exception.PolicyServiceAuthorizationException;
import bio.terra.service.policy.exception.PolicyServiceDuplicateException;
import bio.terra.service.policy.exception.PolicyServiceNotFoundException;
import bio.terra.service.policy.exception.PolicyConflictException;
import io.opencensus.contrib.spring.aop.Traced;
import java.io.IOException;
import java.util.UUID;
import javax.annotation.Nullable;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.slf4j.MDC;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
public class PolicyService {
  private static final Logger logger = LoggerFactory.getLogger(PolicyService.class);
  private final PolicyServiceConfiguration policyServiceConfiguration;

  @Autowired
  public PolicyService(
      PolicyServiceConfiguration policyServiceConfiguration) {
    this.policyServiceConfiguration = policyServiceConfiguration;
    logger.info("TPS base path: '{}'", policyServiceConfiguration.getBasePath());
  }

  // -- Policy Attribute Object Interface --
  @Traced
  public void createSnapshotPao(UUID snapshotId, @Nullable TpsPolicyInputs policyInputs) {
    createPao(snapshotId, TpsObjectType.SNAPSHOT, policyInputs);
  }

  @Traced
  public void createPao(UUID resourceId, TpsObjectType resourceType, @Nullable TpsPolicyInputs policyInputs) {
    policyServiceConfiguration.tpsEnabledCheck();
    TpsPolicyInputs inputs = (policyInputs == null) ? new TpsPolicyInputs() : policyInputs;

    TpsApi tpsApi = policyApi();
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

  @Traced
  public void deletePao(UUID resourceId) {
    policyServiceConfiguration.tpsEnabledCheck();
    TpsApi tpsApi = policyApi();
    try {
      try {
        tpsApi.deletePao(resourceId);
      } catch (ApiException e) {
        throw convertApiException(e);
      }
    } catch (PolicyServiceNotFoundException e) {
      // Not found is not an error as far as WSM is concerned.
    }
  }

  private ApiClient getApiClient(String accessToken) {
    ApiClient client =
        new ApiClient()
            .addDefaultHeader(
                RequestIdFilter.REQUEST_ID_HEADER,
                MDC.get(RequestIdFilter.REQUEST_ID_MDC_KEY));
    client.setAccessToken(accessToken);
    return client;
  }

  private TpsApi policyApi() {
    try {
      return new TpsApi(
          getApiClient(policyServiceConfiguration.getAccessToken())
              .setBasePath(policyServiceConfiguration.getBasePath()));
    } catch (IOException e) {
      throw new PolicyServiceAuthorizationException(
         "Error reading or parsing credentials file",
          e.getCause());
    }
  }

  private RuntimeException convertApiException(ApiException ex) {
    if (ex.getCode() == HttpStatus.UNAUTHORIZED.value()) {
      return new PolicyServiceAuthorizationException(
          "Not authorized to access Terra Policy Service", ex.getCause());
    } else if (ex.getCode() == HttpStatus.NOT_FOUND.value()) {
      return new PolicyServiceNotFoundException("Policy service returns not found exception", ex);
    } else if (ex.getCode() == HttpStatus.BAD_REQUEST.value()
        && StringUtils.containsIgnoreCase(ex.getMessage(), "duplicate")) {
      return new PolicyServiceDuplicateException(
          "Policy service throws duplicate object exception", ex);
    } else if (ex.getCode() == HttpStatus.CONFLICT.value()) {
      return new PolicyConflictException("Policy service throws conflict exception", ex);
    } else {
      return new PolicyServiceAPIException(ex);
    }
  }
}
