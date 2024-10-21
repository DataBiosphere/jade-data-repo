package bio.terra.service.rawls;

import bio.terra.app.configuration.RawlsConfiguration;
import bio.terra.common.iam.AuthenticatedUserRequest;
import com.google.common.annotations.VisibleForTesting;
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
import org.springframework.stereotype.Component;
import org.springframework.web.client.RestTemplate;

@Component
public class RawlsClient {

  private static final Logger logger = LoggerFactory.getLogger(RawlsClient.class);

  private final RawlsConfiguration rawlsConfiguration;
  private final RestTemplate restTemplate;
  private final HttpHeaders headers;

  @Autowired
  public RawlsClient(RawlsConfiguration rawlsConfiguration, RestTemplate restTemplate) {
    this.rawlsConfiguration = rawlsConfiguration;
    this.restTemplate = restTemplate;
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
              getWorkspaceEndpoint(workspaceId),
              HttpMethod.GET,
              new HttpEntity<>(headers),
              WorkspaceResponse.class);
      if (!workspaceCall.getStatusCode().is2xxSuccessful()) {
        logger.warn("Unsuccessful response retrieving workspace {} by {}", workspaceId, userEmail);
      }
      return workspaceCall.getBody();
    } catch (Exception e) {
      logger.warn("Error retrieving workspace", e);
      throw e;
    }
  }

  @VisibleForTesting
  String getWorkspaceEndpoint(UUID workspaceId) {
    return String.format("%s/api/workspaces/id/%s", rawlsConfiguration.basePath(), workspaceId);
  }
}
