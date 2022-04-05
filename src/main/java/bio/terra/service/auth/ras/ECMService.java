package bio.terra.service.auth.ras;

import bio.terra.app.configuration.ECMConfiguration;
import bio.terra.externalcreds.api.PassportApi;
import bio.terra.externalcreds.client.ApiClient;
import bio.terra.externalcreds.model.ValidatePassportRequest;
import bio.terra.externalcreds.model.ValidatePassportResult;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.converter.json.MappingJackson2HttpMessageConverter;
import org.springframework.stereotype.Service;
import org.springframework.web.client.RestTemplate;

@Service
public class ECMService {
  private final Logger logger = LoggerFactory.getLogger(ECMService.class);
  private final ECMConfiguration ecmConfiguration;
  private final MappingJackson2HttpMessageConverter jacksonConverter;

  public ECMService(
      ECMConfiguration ecmConfiguration, MappingJackson2HttpMessageConverter jacksonConverter) {
    this.ecmConfiguration = ecmConfiguration;
    this.jacksonConverter = jacksonConverter;
  }

  public PassportApi getPassportApi() {
    // ECM returns lots of responses as JSON string.
    // By default, these get intercepted by Spring's raw string converter, but they're quoted, so
    // they should be parsed as JSON.
    // N.B. This means that any non-JSON responses won't be parsed correctly, so if ECM ever returns
    // any other content-type, this would have to be revised. However, that seems unlikely.
    var restTemplate = new RestTemplate(List.of(jacksonConverter));

    var client = new ApiClient(restTemplate);
    client.setBasePath(ecmConfiguration.getInstanceUrl());

    return new PassportApi(client);
  }

  public ValidatePassportResult validatePassport(ValidatePassportRequest validatePassportRequest) {
    var passportApi = getPassportApi();
    return passportApi.validatePassport(validatePassportRequest);
  }
}
