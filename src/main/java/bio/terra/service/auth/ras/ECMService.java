package bio.terra.service.auth.ras;

import bio.terra.app.configuration.ECMConfiguration;
import bio.terra.externalcreds.api.PassportApi;
import bio.terra.externalcreds.client.ApiClient;
import bio.terra.externalcreds.model.ValidatePassportRequest;
import bio.terra.externalcreds.model.ValidatePassportResult;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class ECMService {
  private final ECMConfiguration ecmConfiguration;
  private final RestTemplate restTemplate;

  @Autowired
  public ECMService(ECMConfiguration ecmConfiguration, RestTemplate restTemplate) {
    this.ecmConfiguration = ecmConfiguration;
    this.restTemplate = restTemplate;
  }

  public PassportApi getPassportApi() {
    var client = new ApiClient(restTemplate);
    client.setBasePath(ecmConfiguration.getBasePath());

    return new PassportApi(client);
  }

  public ValidatePassportResult validatePassport(ValidatePassportRequest validatePassportRequest) {
    var passportApi = getPassportApi();
    return passportApi.validatePassport(validatePassportRequest);
  }
}
