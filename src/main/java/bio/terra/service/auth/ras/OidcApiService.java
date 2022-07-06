package bio.terra.service.auth.ras;

import bio.terra.app.configuration.EcmConfiguration;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.externalcreds.api.OidcApi;
import bio.terra.externalcreds.client.ApiClient;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class OidcApiService {
  private final EcmConfiguration ecmConfiguration;
  private final RestTemplate restTemplate;

  @Autowired
  public OidcApiService(EcmConfiguration ecmConfiguration, RestTemplate restTemplate) {
    this.ecmConfiguration = ecmConfiguration;
    this.restTemplate = restTemplate;
  }

  public OidcApi getOidcApi(AuthenticatedUserRequest userReq) {
    var client = new ApiClient(restTemplate);
    client.setBasePath(ecmConfiguration.getBasePath());
    client.setAccessToken(userReq.getToken());

    return new OidcApi(client);
  }
}
