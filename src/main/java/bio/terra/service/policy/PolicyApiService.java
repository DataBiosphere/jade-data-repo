package bio.terra.service.policy;

import bio.terra.app.configuration.PolicyServiceConfiguration;
import bio.terra.policy.api.PublicApi;
import bio.terra.policy.api.TpsApi;
import bio.terra.policy.client.ApiClient;
import bio.terra.service.policy.exception.PolicyServiceAuthorizationException;
import java.io.IOException;
import javax.ws.rs.client.Client;
import org.springframework.stereotype.Service;

@Service
public class PolicyApiService {
  private final PolicyServiceConfiguration policyServiceConfiguration;
  /** Clients should be shared among requests to reduce latency and save memory * */
  private final Client sharedHttpClient;

  public PolicyApiService(PolicyServiceConfiguration policyServiceConfiguration) {
    this.policyServiceConfiguration = policyServiceConfiguration;
    this.sharedHttpClient = new ApiClient().getHttpClient();
  }

  // -- Policy Attribute Object Interface --

  private ApiClient getApiClient() {
    return new ApiClient()
        .setHttpClient(sharedHttpClient)
        .setBasePath(policyServiceConfiguration.getBasePath());
  }

  private ApiClient getApiClient(String accessToken) {
    ApiClient client = getApiClient();
    client.setAccessToken(accessToken);
    return client;
  }

  public PublicApi getUnauthPolicyApi() {
    return new PublicApi(getApiClient());
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
