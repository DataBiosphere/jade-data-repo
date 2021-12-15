package bio.terra.app.logging;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import com.google.gson.Gson;
import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.HandlerInterceptor;

@Component
public class LoggerInterceptor implements HandlerInterceptor {
  private static Logger logger = LoggerFactory.getLogger(LoggerInterceptor.class);
  private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;

  @Autowired
  public LoggerInterceptor(AuthenticatedUserRequestFactory authenticatedUserRequestFactory) {
    this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
  }

  @Override
  public void afterCompletion(
      HttpServletRequest request, HttpServletResponse response, Object handler, Exception ex)
      throws Exception {
    AuthenticatedUserRequest userReq = authenticatedUserRequestFactory.from(request);
    String userId = userReq.getSubjectId();
    String userEmail = userReq.getEmail();
    String institute = userEmail != null ? userEmail.substring(userEmail.indexOf("@") + 1) : null;
    String url = request.getRequestURL().toString();
    String method = request.getMethod();
    Map<String, String[]> paramMap = request.getParameterMap();
    Gson gson = new Gson();
    String paramString = gson.toJson(paramMap);
    String responseStatus = Integer.toString(response.getStatus());

    // skip logging the status endpoint
    if (!url.endsWith("/status")) {
      logger.info(
          "userId: {}, email: {}, institute: {}, url: {}, method: {}, params: {}, status: {}",
          userId,
          userEmail,
          institute,
          url,
          method,
          paramString,
          responseStatus);
    }

    if (ex != null) {
      logger.error("An error occurred processing this request: ", ex);
    }
  }
}
