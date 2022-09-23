package bio.terra.app.usermetrics;

import bio.terra.app.configuration.UserMetricsConfiguration;
import bio.terra.common.iam.AuthenticatedUserRequest;
import com.google.common.annotations.VisibleForTesting;
import java.util.Arrays;
import java.util.Collections;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.commons.collections4.map.PassiveExpiringMap;
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

  private static final int DEFAULT_BEARER_TOKEN_CACHE_TIMEOUT_SECONDS = 3600;

  private final Map<String, String> bearerCache;

  private static final String API_PATH = "/api/event";
  private static final String SYNC_PATH = "/api/syncProfile";

  @Autowired
  public BardClient(UserMetricsConfiguration metricsConfig) {
    this.restTemplate = new RestTemplate();
    restTemplate.setRequestFactory(new HttpComponentsClientHttpRequestFactory());

    this.headers = new HttpHeaders();
    headers.setContentType(MediaType.APPLICATION_JSON);
    headers.setAccept(Arrays.asList(MediaType.APPLICATION_JSON, MediaType.APPLICATION_JSON));

    int ttl =
        Objects.requireNonNullElse(
            metricsConfig.getSyncRefreshIntervalSeconds(),
            DEFAULT_BEARER_TOKEN_CACHE_TIMEOUT_SECONDS);
    this.bearerCache = Collections.synchronizedMap(new PassiveExpiringMap<>(ttl, TimeUnit.SECONDS));

    this.metricsConfig = metricsConfig;
  }

  public void logEvent(AuthenticatedUserRequest userReq, BardEvent event) {
    HttpHeaders authedHeaders = new HttpHeaders(headers);
    authedHeaders.setBearerAuth(userReq.getToken());
    syncUser(userReq);
    try {
      ResponseEntity<Void> eventCall =
          getRestTemplate()
              .exchange(
                  getApiURL(), HttpMethod.POST, new HttpEntity<>(event, authedHeaders), Void.class);
      if (!eventCall.getStatusCode().is2xxSuccessful()) {
        logger.warn(
            "Error logging event {}%n{}",
            event.getEvent(), eventCall.getStatusCode().getReasonPhrase());
      }
    } catch (Exception e) {
      logger.warn("Error logging event {}", event.getEvent(), e);
    }
  }

  /**
   * Syncing a user is only needed when a new token is not already cached or the cache entry has
   * expired.
   *
   * @param userReq - the AuthenticatedUserRequest that represents the current user.
   */
  private void syncUser(AuthenticatedUserRequest userReq) {
    String key = userReq.getToken();
    bearerCache.computeIfAbsent(key, k -> syncProfile(userReq) ? "" : null);
  }

  /**
   * Syncs profile info from orchestration to mixpanel to improve querying/reporting capabilities in
   * the mixpanel reports.
   *
   * @param userReq - the user to sync.
   * @return boolean - if the sync request was successful.
   */
  boolean syncProfile(AuthenticatedUserRequest userReq) {
    boolean result = false;
    HttpHeaders authedHeaders = new HttpHeaders(headers);
    authedHeaders.setBearerAuth(userReq.getToken());
    try {
      ResponseEntity<Void> syncCall =
          getRestTemplate()
              .exchange(
                  getSyncPathURL(),
                  HttpMethod.POST,
                  new HttpEntity<>(null, authedHeaders),
                  Void.class);
      if (!syncCall.getStatusCode().is2xxSuccessful()) {
        logger.warn(
            "Error calling sync for user {}%n{}",
            userReq.getEmail(), syncCall.getStatusCode().getReasonPhrase());
      } else {
        result = true;
      }
    } catch (Exception e) {
      logger.warn(
          "Unable to sync profile for user {} token {} because {}",
          userReq.getEmail(),
          userReq.getToken(),
          e.getMessage());
    }
    return result;
  }

  @VisibleForTesting
  RestTemplate getRestTemplate() {
    return restTemplate;
  }

  @VisibleForTesting
  String getApiURL() {
    return metricsConfig.getBardBasePath() + API_PATH;
  }

  String getSyncPathURL() {
    return metricsConfig.getBardBasePath() + SYNC_PATH;
  }
}
