package bio.terra.app.configuration;

import bio.terra.app.controller.AuthenticatedUserRequest;
import bio.terra.app.controller.AuthenticatedUserRequestFactory;
import com.google.gson.Gson;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.springframework.web.servlet.handler.HandlerInterceptorAdapter;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.Map;

@Component
public class LoggerInterceptor extends HandlerInterceptorAdapter {
    private static Logger logger = LoggerFactory.getLogger(LoggerInterceptor.class);
    private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;

    @Autowired
    public LoggerInterceptor(
        AuthenticatedUserRequestFactory authenticatedUserRequestFactory
    ) {
        this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
    }

    @Override
    public void afterCompletion(
        HttpServletRequest request,
        HttpServletResponse response,
        Object handler,
        Exception ex
    ) throws Exception {
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
            logger.info("userId: {}, email: {}, institute: {}, url: {}, method: {}, params: {}, status: {}",
                userId, userEmail, institute, url, method, paramString, responseStatus);
        }

        if (ex != null) {
            logger.error("An error occurred processing this request: ", ex);
        }
    }
}
