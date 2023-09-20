package bio.terra.service.duos;

import bio.terra.app.configuration.DuosConfiguration;
import bio.terra.common.exception.ErrorReportException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.oauth2.GoogleCredentialsService;
import bio.terra.service.duos.exception.DuosDatasetBadRequestException;
import bio.terra.service.duos.exception.DuosDatasetNotFoundException;
import bio.terra.service.duos.exception.DuosInternalServerErrorException;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.HttpStatusCodeException;
import org.springframework.web.client.RestTemplate;

@Component
public class DuosClient {

  private static final Logger logger = LoggerFactory.getLogger(DuosClient.class);
  static final List<String> SCOPES = List.of("email", "profile");

  private final DuosConfiguration duosConfiguration;
  private final RestTemplate restTemplate;
  private final GoogleCredentialsService googleCredentialsService;

  public DuosClient(
      DuosConfiguration duosConfiguration,
      RestTemplate restTemplate,
      GoogleCredentialsService googleCredentialsService) {
    this.duosConfiguration = duosConfiguration;
    this.restTemplate = restTemplate;
    restTemplate.setRequestFactory(new HttpComponentsClientHttpRequestFactory());
    this.googleCredentialsService = googleCredentialsService;
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
  String getStatusUrl() {
    return String.format("%s/status", duosConfiguration.basePath());
  }

  /**
   * @return status of DUOS and its subsystems
   */
  public SystemStatus status() {
    HttpHeaders unauthedHeaders = getHttpHeaders();
    var statusCall =
        restTemplate.exchange(
            getStatusUrl(), HttpMethod.GET, new HttpEntity<>(unauthedHeaders), SystemStatus.class);
    return statusCall.getBody();
  }

  @VisibleForTesting
  String getDatasetUrl(String duosId) {
    return String.format("%s/api/tdr/%s", duosConfiguration.basePath(), duosId);
  }

  /**
   * @param duosId DUOS dataset ID
   * @param userRequest authenticated user
   * @return the DuosDataset associated with duosId
   */
  public DuosDataset getDataset(String duosId, AuthenticatedUserRequest userRequest) {
    HttpHeaders authedHeaders = getHttpHeaders();
    authedHeaders.setBearerAuth(userRequest.getToken());
    String url = getDatasetUrl(duosId);
    logger.debug("About to GET {} as authenticated user", url);
    try {
      var datasetCall =
          restTemplate.exchange(
              url, HttpMethod.GET, new HttpEntity<>(authedHeaders), DuosDataset.class);
      return datasetCall.getBody();
    } catch (HttpStatusCodeException ex) {
      throw convertToDataRepoException(ex, duosId);
    }
  }

  @VisibleForTesting
  String getApprovedUsersUrl(String duosId) {
    return getDatasetUrl(duosId) + "/approved/users";
  }

  /**
   * This call to DUOS is performed as the TDR SA rather than a supplied authenticated user.
   *
   * @param duosId DUOS dataset ID
   * @return all users who have access to the dataset via an accepted DAR
   */
  public DuosDatasetApprovedUsers getApprovedUsers(String duosId) {
    HttpHeaders saHeaders = getHttpHeaders();
    saHeaders.setBearerAuth(googleCredentialsService.getApplicationDefaultAccessToken(SCOPES));
    String url = getApprovedUsersUrl(duosId);
    logger.debug("About to GET {} as TDR SA", url);
    try {
      var approvedUsersCall =
          restTemplate.exchange(
              url, HttpMethod.GET, new HttpEntity<>(saHeaders), DuosDatasetApprovedUsers.class);
      return approvedUsersCall.getBody();
    } catch (HttpStatusCodeException ex) {
      throw convertToDataRepoException(ex, duosId);
    }
  }

  @VisibleForTesting
  static ErrorReportException convertToDataRepoException(
      HttpStatusCodeException duosEx, String duosId) {
    return switch (duosEx.getStatusCode()) {
      case BAD_REQUEST -> new DuosDatasetBadRequestException(
          "DUOS dataset identifier %s is malformed".formatted(duosId), duosEx);
      case NOT_FOUND -> new DuosDatasetNotFoundException(
          "Could not find DUOS dataset for identifier %s".formatted(duosId), duosEx);
      default -> new DuosInternalServerErrorException("Unexpected error from DUOS", duosEx);
    };
  }
}
