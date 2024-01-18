package bio.terra.app.logging;

import bio.terra.common.exception.UnauthorizedException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import com.google.gson.Gson;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.servlet.HandlerInterceptor;
import org.springframework.web.util.ContentCachingRequestWrapper;

@Component
public class LoggerInterceptor implements HandlerInterceptor {
  private static final Logger logger = LoggerFactory.getLogger(LoggerInterceptor.class);
  // Constants for requests coming in that aren't authenticated
  private static final String UNAUTHED_USER_ID = "N/A";
  private static final String UNAUTHED_EMAIL = "N/A";
  private static final String UNAUTHED_INSTITUTE = "N/A";
  // Don't log requests for URLs that end with any of   the following paths
  private static final Set<String> LOG_EXCLUDE_LIST = Set.of("/status");

  private static final String REQUEST_START_ATTRIBUTE = "x-request-start";
  private static final long NOT_FOUND_DURATION = -1;

  private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;

  // A Java char is 16 bits, and Stackdiver's limit is 256kb.
  // Although this works out to 128,000 chars, we limit to 100,000 to allow for the rest of
  // the log message
  private static final int STACKDRIVER_MAX_CHARS = 100000;

  @Autowired
  public LoggerInterceptor(AuthenticatedUserRequestFactory authenticatedUserRequestFactory) {
    this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
  }

  @Override
  public boolean preHandle(HttpServletRequest request, HttpServletResponse response, Object handler)
      throws Exception {
    request.setAttribute(REQUEST_START_ATTRIBUTE, System.currentTimeMillis());
    return true;
  }

  @Override
  public void afterCompletion(
      HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex)
      throws Exception {

    String userId;
    String userEmail;
    String institute;
    try {
      AuthenticatedUserRequest userReq = authenticatedUserRequestFactory.from(request);
      userId = userReq.getSubjectId();
      userEmail = userReq.getEmail();
      institute = userEmail != null ? userEmail.substring(userEmail.indexOf("@") + 1) : null;
    } catch (UnauthorizedException e) {
      userId = UNAUTHED_USER_ID;
      userEmail = UNAUTHED_EMAIL;
      institute = UNAUTHED_INSTITUTE;
    }

    String url = request.getRequestURL().toString();
    String method = request.getMethod();
    Map<String, String[]> paramMap = request.getParameterMap();
    Gson gson = new Gson();
    String paramString = gson.toJson(paramMap);
    String responseStatus = Integer.toString(response.getStatus());
    long requestDuration;
    Long requestStartTime = (Long) request.getAttribute(REQUEST_START_ATTRIBUTE);
    if (requestStartTime != null) {
      requestDuration = System.currentTimeMillis() - requestStartTime;
    } else {
      requestDuration = NOT_FOUND_DURATION;
    }
    // skip logging the status endpoint
    if (LOG_EXCLUDE_LIST.stream().noneMatch(url::endsWith)) {

      Map<String, String> stackDriverPayload = new HashMap<>();
      if (RequestMethod.POST.name().equalsIgnoreCase(method)
          || RequestMethod.PUT.name().equalsIgnoreCase(method)) {
        String requestBody =
            new String(
                ((ContentCachingRequestWrapper) request).getContentAsByteArray(),
                StandardCharsets.UTF_8);
        String requestBodyPayload =
            requestBody.length() > STACKDRIVER_MAX_CHARS
                ? requestBody.substring(0, STACKDRIVER_MAX_CHARS)
                : requestBody;
        stackDriverPayload.put("requestBody", requestBodyPayload);
      }
      // The warning here can be ignored, as StackDriver logging will interpret the extra arg
      // and insert it into the `jsonPayload`. It is ignored when running locally.
      // This copies the Terra Commons logging, which relies on the GoogleJsonLayout.
      // See bio.terra.common.logging.RequestLoggingFilter.doFilter:92 for another example.
      // Also see
      // https://github.com/DataBiosphere/terra-common-lib/blob/develop/src/main/java/bio/terra/common/logging/GoogleJsonLayout.java
      // for the layout implementation.
      logger.info(
          "userId: {}, email: {}, institute: {}, url: {}, method: {}, params: {}, status: {}, duration: {}",
          userId,
          userEmail,
          institute,
          url,
          method,
          paramString,
          responseStatus,
          requestDuration,
          stackDriverPayload);
    }

    if (ex != null) {
      logger.error("An error occurred processing this request: ", ex);
    }
  }
}
