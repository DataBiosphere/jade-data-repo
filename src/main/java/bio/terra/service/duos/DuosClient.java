package bio.terra.service.duos;

import bio.terra.app.configuration.DuosConfiguration;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.duos.exception.DuosDatasetBadRequestException;
import bio.terra.service.duos.exception.DuosDatasetNotFoundException;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpClientErrorException;
import org.springframework.web.client.RestTemplate;

@Component
public class DuosClient {

  private final DuosConfiguration duosConfiguration;
  private final RestTemplate restTemplate;

  public DuosClient(DuosConfiguration duosConfiguration, RestTemplate restTemplate) {
    this.duosConfiguration = duosConfiguration;
    this.restTemplate = restTemplate;
    restTemplate.setRequestFactory(new HttpComponentsClientHttpRequestFactory());
  }

  /**
   * @return a new unauthenticated HttpHeaders object for interacting with DUOS APIs
   */
  private HttpHeaders getHttpHeaders() {
    HttpHeaders headers = new HttpHeaders();
    headers.setAccept(List.of(MediaType.APPLICATION_JSON));
    return headers;
  }

  @VisibleForTesting
  String getDatasetUrl(String duosId) {
    return String.format("%s/api/tdr/%s", duosConfiguration.basePath(), duosId);
  }

  /**
   * @param duosId DUOS dataset ID
   * @param userRequest authenticated user
   * @return the DuosDataset associated with duosId
   * @throws DuosDatasetBadRequestException if identifier is malformed
   * @throws DuosDatasetNotFoundException if no DUOS dataset found
   */
  public DuosDataset getDataset(String duosId, AuthenticatedUserRequest userRequest) {
    HttpHeaders authedHeaders = getHttpHeaders();
    authedHeaders.setBearerAuth(userRequest.getToken());
    try {
      ResponseEntity<DuosDataset> datasetCall =
          restTemplate.exchange(
              getDatasetUrl(duosId),
              HttpMethod.GET,
              new HttpEntity<>(authedHeaders),
              DuosDataset.class);
      return datasetCall.getBody();
    } catch (HttpClientErrorException ex) {
      switch (ex.getStatusCode()) {
        case BAD_REQUEST -> throw new DuosDatasetBadRequestException(
            "DUOS dataset identifier %s is malformed".formatted(duosId), ex);
        case NOT_FOUND -> throw new DuosDatasetNotFoundException(
            "Could not find DUOS dataset for identifier %s".formatted(duosId), ex);
        default -> throw ex;
      }
    }
  }
}
