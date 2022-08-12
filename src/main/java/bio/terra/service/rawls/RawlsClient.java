package bio.terra.service.rawls;

import bio.terra.app.configuration.RawlsConfiguration;
import bio.terra.app.model.rawls.WorkspaceResponse;
import bio.terra.common.iam.AuthenticatedUserRequest;
import java.util.List;
import java.util.UUID;
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
public class RawlsClient {

  private static final Logger logger = LoggerFactory.getLogger(RawlsClient.class);

  private final RawlsConfiguration rawlsConfiguration;
  private final RestTemplate restTemplate;
  private final HttpHeaders headers;

  @Autowired
  public RawlsClient(RawlsConfiguration rawlsConfiguration) {
    this.rawlsConfiguration = rawlsConfiguration;
    this.restTemplate = new RestTemplate();
    restTemplate.setRequestFactory(new HttpComponentsClientHttpRequestFactory());
    this.headers = new HttpHeaders();
    headers.setAccept(List.of(MediaType.APPLICATION_JSON));
  }

  public WorkspaceResponse getWorkspace(UUID workspaceId, AuthenticatedUserRequest userRequest) {
    HttpHeaders authedHeaders = new HttpHeaders(headers);
    authedHeaders.setBearerAuth(userRequest.getToken());
    String userEmail = userRequest.getEmail();
    try {
      ResponseEntity<WorkspaceResponse> workspaceCall =
          restTemplate.exchange(
              String.format(
                  "%s/api/workspaces/id/%s", rawlsConfiguration.getBasePath(), workspaceId),
              HttpMethod.GET,
              new HttpEntity<>(headers),
              WorkspaceResponse.class);
      if (!workspaceCall.getStatusCode().is2xxSuccessful()) {
        logger.warn("Unsuccessful response retrieving workspace {} by {}", workspaceId, userEmail);
      }
      return workspaceCall.getBody();
    } catch (Exception e) {
      logger.warn("Error retrieving workspace {} by {}", workspaceId, userEmail);
      throw e;
    }
  }
}
