package bio.terra.app.usermetrics;

import bio.terra.app.configuration.UserMetricsConfiguration;
import bio.terra.common.iam.AuthenticatedUserRequest;
import java.util.Arrays;
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

/** Wraps HTTP calls to communicate with the Bard Http service */
@Component
public class BardClient {

  private static final Logger logger = LoggerFactory.getLogger(BardClient.class);

  private final UserMetricsConfiguration metricsConfig;

  private final RestTemplate restTemplate;
  private final HttpHeaders headers;

  @Autowired
  public BardClient(UserMetricsConfiguration metricsConfig) {
    this.restTemplate = new RestTemplate();
    restTemplate.setRequestFactory(new HttpComponentsClientHttpRequestFactory());
    this.headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    headers.setAccept(Arrays.asList(MediaType.APPLICATION_JSON, MediaType.APPLICATION_JSON));

    this.metricsConfig = metricsConfig;
  }

  public void logEvent(AuthenticatedUserRequest userReq, BardEvent event) {
    HttpHeaders authedHeaders = new HttpHeaders(headers);
    authedHeaders.setBearerAuth(userReq.getToken());
    try {
      ResponseEntity<Void> eventCall =
          restTemplate.exchange(
              metricsConfig.getBardBasePath() + "/api/event",
              HttpMethod.POST,
              new HttpEntity<>(event, authedHeaders),
              Void.class);
      if (!eventCall.getStatusCode().is2xxSuccessful()) {
        logger.warn(
            "Error logging event {}%n{}",
            event.getEvent(), eventCall.getStatusCode().getReasonPhrase());
      }
    } catch (Exception e) {
      logger.warn("Error logging event {}", event.getEvent(), e);
    }
  }
}
