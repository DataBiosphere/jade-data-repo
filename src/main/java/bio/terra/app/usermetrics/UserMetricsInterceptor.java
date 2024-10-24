package bio.terra.app.usermetrics;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.configuration.UserMetricsConfiguration;
import bio.terra.common.exception.UnauthorizedException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

@Component
public class UserMetricsInterceptor implements HandlerInterceptor {
  static final String API_EVENT_NAME = "tdr:api";

  private final BardClient bardClient;
  private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  private final ApplicationConfiguration applicationConfiguration;
  private final UserMetricsConfiguration metricsConfig;
  private final ExecutorService metricsPerformanceThreadpool;
  private final UserLoggingMetrics eventProperties;

  @Autowired
  public UserMetricsInterceptor(
      BardClient bardClient,
      AuthenticatedUserRequestFactory authenticatedUserRequestFactory,
      ApplicationConfiguration applicationConfiguration,
      UserMetricsConfiguration metricsConfig,
      UserLoggingMetrics eventProperties,
      @Qualifier("metricsReportingThreadpool") ExecutorService metricsPerformanceThreadpool) {
    this.bardClient = bardClient;
    this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
    this.applicationConfiguration = applicationConfiguration;
    this.metricsConfig = metricsConfig;
    this.metricsPerformanceThreadpool = metricsPerformanceThreadpool;
    this.eventProperties = eventProperties;
  }

  @Override
  public void afterCompletion(
      HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex) {
    String method = request.getMethod().toUpperCase();
    String path = request.getRequestURI();
    AuthenticatedUserRequest userRequest;
    try {
      userRequest = authenticatedUserRequestFactory.from(request);
    } catch (UnauthorizedException e) {
      // Don't track unauthenticated requests
      return;
    }

    // Don't log metrics if bard isn't configured or the path is part of the ignore-list
    if (StringUtils.isEmpty(metricsConfig.bardBasePath()) || ignoreEventForPath(path)) {
      return;
    }

    HashMap<String, Object> properties =
        new HashMap<>(
            Map.of(
                BardEventProperties.METHOD_FIELD_NAME, method,
                BardEventProperties.PATH_FIELD_NAME, path));
    addToPropertiesIfPresentInHeader(
        request, properties, "X-Transaction-Id", BardEventProperties.TRANSACTION_ID_FIELD_NAME);
    eventProperties.setAll(properties);
    HashMap<String, Object> bardEventProperties = eventProperties.get();

    // Spawn a thread so that sending the metric doesn't slow down the initial request
    metricsPerformanceThreadpool.submit(
        () ->
            bardClient.logEvent(
                userRequest,
                new BardEvent(
                    API_EVENT_NAME,
                    bardEventProperties,
                    metricsConfig.appId(),
                    applicationConfiguration.getDnsName())));
  }

  /** Should we actually ignore sending a tracking event for this path */
  private boolean ignoreEventForPath(String path) {
    return metricsConfig.ignorePaths().stream().anyMatch(p -> FilenameUtils.wildcardMatch(path, p));
  }

  /**
   * If a given header is present in the request and has a value, add it to the properties map to be
   * tracked in Bard
   *
   * @param request The request being logged
   * @param properties The properties map that will be sent to Bard for tracking
   * @param headerName The name of the header to examine
   * @param propertyName The name of the map key to use when adding the header value to the
   *     properties map
   */
  private void addToPropertiesIfPresentInHeader(
      HttpServletRequest request,
      Map<String, Object> properties,
      String headerName,
      String propertyName) {
    var headerValue = request.getHeader(headerName);
    if (headerValue != null) {
      properties.put(propertyName, headerValue);
    }
  }
}
