package bio.terra.app.usermetrics;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.configuration.UserMetricsConfiguration;
import bio.terra.common.exception.UnauthorizedException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import liquibase.util.StringUtils;
import org.apache.commons.io.FilenameUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.util.ContentCachingRequestWrapper;

@Component
public class UserMetricsInterceptor implements HandlerInterceptor {
  static final String API_EVENT_NAME = "tdr:api";
  static final String METHOD_FIELD_NAME = "method";
  static final String PATH_FIELD_NAME = "path";
  static final String BILLING_PROFILE_ID_FIELD_NAME = "billingProfileId";

  // Request parameters that contain the billing profile id
  static final String DEFAULT_BILLING_PROFILE_ID_FIELD_NAME = "defaultProfileId";
  static final String PROFILE_ID_FIELD_NAME = "profileId";
  static final List<String> PROFILE_FIELDS =
      List.of(DEFAULT_BILLING_PROFILE_ID_FIELD_NAME, PROFILE_ID_FIELD_NAME);
  private static final Logger logger = LoggerFactory.getLogger(UserMetricsInterceptor.class);

  private final BardClient bardClient;
  private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  private final ApplicationConfiguration applicationConfiguration;
  private final UserMetricsConfiguration metricsConfig;
  private final ExecutorService metricsPerformanceThreadpool;
  private final ObjectMapper objectMapper;

  @Autowired
  public UserMetricsInterceptor(
      BardClient bardClient,
      AuthenticatedUserRequestFactory authenticatedUserRequestFactory,
      ApplicationConfiguration applicationConfiguration,
      UserMetricsConfiguration metricsConfig,
      @Qualifier("metricsReportingThreadpool") ExecutorService metricsPerformanceThreadpool,
      @Qualifier("objectMapper") ObjectMapper objectMapper) {
    this.bardClient = bardClient;
    this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
    this.applicationConfiguration = applicationConfiguration;
    this.metricsConfig = metricsConfig;
    this.metricsPerformanceThreadpool = metricsPerformanceThreadpool;
    this.objectMapper = objectMapper;
  }

  private HashMap<String, Object> getEventProperties(HttpServletRequest request) {
    String method = request.getMethod().toUpperCase();
    String path = request.getRequestURI();
    HashMap<String, Object> eventProperties =
        new HashMap<>(
            Map.of(
                METHOD_FIELD_NAME, method,
                PATH_FIELD_NAME, path));
    if (method.equals("POST")) {
      try {
        Optional<String> profileId = getProfileIdFromRequest(request);
        profileId.ifPresent(s -> eventProperties.put(BILLING_PROFILE_ID_FIELD_NAME, s));
      } catch (JsonProcessingException e) {
        logger.info("Could not parse billing profile id from request");
      }
    }
    return eventProperties;
  }

  @VisibleForTesting
  public ContentCachingRequestWrapper getRequestWrapper(HttpServletRequest request) {
    return new ContentCachingRequestWrapper(request);
  }

  @VisibleForTesting
  public Optional<String> getProfileIdFromRequest(HttpServletRequest request)
      throws JsonProcessingException {
    ContentCachingRequestWrapper requestWrapper = getRequestWrapper(request);
    String payload = new String(requestWrapper.getContentAsByteArray(), StandardCharsets.UTF_8);
    JsonNode jsonNode = objectMapper.readTree(payload);
    return PROFILE_FIELDS.stream()
        .map(jsonNode::get)
        .filter(Objects::nonNull)
        .map(JsonNode::asText)
        .findFirst();
  }

  @Override
  public void afterCompletion(
      HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex)
      throws Exception {
    String path = request.getRequestURI();
    AuthenticatedUserRequest userRequest;
    try {
      userRequest = authenticatedUserRequestFactory.from(request);
    } catch (UnauthorizedException e) {
      // Don't track unauthenticated requests
      return;
    }

    // Don't log metrics if bard isn't configured or the path is part of the ignore-list
    if (StringUtils.isEmpty(metricsConfig.getBardBasePath()) || ignoreEventForPath(path)) {
      return;
    }

    // Spawn a thread so that sending the metric doesn't slow down the initial request
    metricsPerformanceThreadpool.submit(
        () ->
            bardClient.logEvent(
                userRequest,
                new BardEvent(
                    API_EVENT_NAME,
                    getEventProperties(request),
                    metricsConfig.getAppId(),
                    applicationConfiguration.getDnsName())));
  }

  /** Should we actually ignore sending a tracking event for this path */
  private boolean ignoreEventForPath(String path) {
    return metricsConfig.getIgnorePaths().stream()
        .anyMatch(p -> FilenameUtils.wildcardMatch(path, p));
  }
}
