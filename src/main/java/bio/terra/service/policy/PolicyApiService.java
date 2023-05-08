package bio.terra.service.policy;

import bio.terra.app.configuration.PolicyServiceConfiguration;
import bio.terra.policy.api.PublicApi;
import bio.terra.policy.api.TpsApi;
import bio.terra.policy.client.ApiClient;
import bio.terra.service.policy.exception.PolicyServiceAuthorizationException;
import java.io.IOException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

@Service
public class PolicyApiService {
  private final PolicyServiceConfiguration policyServiceConfiguration;

  @Autowired
  public PolicyApiService(PolicyServiceConfiguration policyServiceConfiguration) {
    this.policyServiceConfiguration = policyServiceConfiguration;
  }

  // -- Policy Attribute Object Interface --

  private ApiClient getApiClient(String accessToken) {
    ApiClient client = new ApiClient();
    client.setAccessToken(accessToken);
    client.setBasePath(policyServiceConfiguration.getBasePath());
    return client;
  }

  public PublicApi getUnauthPolicyApi() {
    ApiClient client = new ApiClient();
    client.setBasePath(policyServiceConfiguration.getBasePath());
    return new PublicApi(client);
  }

  public TpsApi getPolicyApi() {
    try {
      return new TpsApi(getApiClient(policyServiceConfiguration.getAccessToken()));
    } catch (IOException e) {
      throw new PolicyServiceAuthorizationException(
          "Error reading or parsing credentials file", e.getCause());
    }
  }
}
