package bio.terra.app.usermetrics;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.configuration.UserMetricsConfiguration;
import bio.terra.common.exception.UnauthorizedException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import liquibase.util.StringUtils;
import org.apache.commons.io.FilenameUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

@Component
public class UserMetricsInterceptor implements HandlerInterceptor {
  static final String API_EVENT_NAME = "tdr:api";
  public static ThreadLocal<HashMap<String, Object>> eventProperties =
      ThreadLocal.withInitial(() -> new HashMap<>());
  private final BardClient bardClient;
  private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  private final ApplicationConfiguration applicationConfiguration;
  private final UserMetricsConfiguration metricsConfig;
  private final ExecutorService metricsPerformanceThreadpool;

  @Autowired
  public UserMetricsInterceptor(
      BardClient bardClient,
      AuthenticatedUserRequestFactory authenticatedUserRequestFactory,
      ApplicationConfiguration applicationConfiguration,
      UserMetricsConfiguration metricsConfig,
      @Qualifier("metricsReportingThreadpool") ExecutorService metricsPerformanceThreadpool) {
    this.bardClient = bardClient;
    this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
    this.applicationConfiguration = applicationConfiguration;
    this.metricsConfig = metricsConfig;
    this.metricsPerformanceThreadpool = metricsPerformanceThreadpool;
  }

  @Override
  public void afterCompletion(
      HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex)
      throws Exception {
    String method = request.getMethod().toUpperCase();
    String path = request.getRequestURI();
    AuthenticatedUserRequest userRequest;
    try {
      userRequest = authenticatedUserRequestFactory.from(request);
    } catch (UnauthorizedException e) {
      // Don't track unauthenticated requests
      return;
    }

    HashMap<String, Object> properties =
        new HashMap<>(
            Map.of(
                BardEventProperties.METHOD_FIELD_NAME, method,
                BardEventProperties.PATH_FIELD_NAME, path));
    properties.putAll(eventProperties.get());

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
                    properties,
                    metricsConfig.getAppId(),
                    applicationConfiguration.getDnsName())));
  }

  /** Should we actually ignore sending a tracking event for this path */
  private boolean ignoreEventForPath(String path) {
    return metricsConfig.getIgnorePaths().stream()
        .anyMatch(p -> FilenameUtils.wildcardMatch(path, p));
  }
}
