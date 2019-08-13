package bio.terra.configuration;

import bio.terra.controller.AuthenticatedUserRequest;
import bio.terra.controller.AuthenticatedUserRequestFactory;
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
        final String userEmail = userReq.getEmail();
        final String institute = userEmail.substring(userEmail.indexOf("@") + 1);
        final String url = request.getRequestURL().toString();
        final String method = request.getMethod();
        final Map<String, String[]> paramMap = request.getParameterMap();
        final Gson gson = new Gson();
        final String paramString = gson.toJson(paramMap);
        final String responseStatus = Integer.toString(response.getStatus());

        logger.info("email: {}, institute: {}, url: {}, method: {}, params: {}, status: {}",
            userEmail, institute, url, method, paramString, responseStatus);

        if (ex != null) {
            logger.error("An error occurred processing this request: ", ex);
        }
    }
}
