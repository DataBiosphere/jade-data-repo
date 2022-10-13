package bio.terra.service.duos;

import bio.terra.app.configuration.DuosConfiguration;
import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpMethod;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.http.client.HttpComponentsClientHttpRequestFactory;
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class DuosClient {

  private static final Logger logger = LoggerFactory.getLogger(DuosClient.class);

  private static final List<String> DUOS_SCOPES = List.of("email", "profile");

  private final DuosConfiguration duosConfiguration;
  private final RestTemplate restTemplate;

  @Autowired
  public DuosClient(DuosConfiguration duosConfiguration) {
    this.duosConfiguration = duosConfiguration;
    this.restTemplate = new RestTemplate();
    restTemplate.setRequestFactory(new HttpComponentsClientHttpRequestFactory());
  }

  /**
   * We construct a new object each time because a shallow copy can produce unexpected results.
   *
   * @return
   */
  private HttpHeaders getHttpHeaders() {
    HttpHeaders headers = new HttpHeaders();
    headers.setAccept(List.of(MediaType.APPLICATION_JSON));
    try {
      // TODO - break out in util for reuse?
      GoogleCredentials credentials = GoogleCredentials.getApplicationDefault();
      if (credentials.createScopedRequired()) {
        credentials = credentials.createScoped(DUOS_SCOPES);
      }
      AccessToken token = credentials.refreshAccessToken();
      headers.setBearerAuth(token.getTokenValue());
    } catch (Exception ex) {
      logger.error("TODO - need to handle this exception better", ex);
    }
    return headers;
  }

  public record ApprovedUser(String email) {}

  public record ApprovedUsers(List<ApprovedUser> approvedUsers) {}

  public ApprovedUsers getApprovedUsers(String duosId) {
    HttpHeaders headers = getHttpHeaders();
    try {
      ResponseEntity<ApprovedUsers> approvedUsersCall =
          restTemplate.exchange(
              String.format(
                  "%s/api/tdr/%s/approved/users", duosConfiguration.getBasePath(), duosId),
              HttpMethod.GET,
              new HttpEntity<>(headers),
              ApprovedUsers.class);
      if (!approvedUsersCall.getStatusCode().is2xxSuccessful()) {
        logger.warn("Unsuccessful response retrieving users for DUOS dataset {}", duosId);
      }
      return approvedUsersCall.getBody();
    } catch (Exception ex) {
      logger.warn("Error retrieving users for DUOS dataset {}", duosId);
      throw ex;
    }
  }
}
