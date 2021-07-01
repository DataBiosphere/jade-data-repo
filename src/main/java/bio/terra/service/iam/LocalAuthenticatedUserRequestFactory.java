package bio.terra.service.iam;

import bio.terra.app.configuration.ApplicationConfiguration;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import okhttp3.OkHttpClient;
import okhttp3.Request;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import javax.servlet.http.HttpServletRequest;
import java.io.IOException;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;

@Component
public class LocalAuthenticatedUserRequestFactory implements AuthenticatedUserRequestFactory {
    private Logger logger = LoggerFactory.getLogger(LocalAuthenticatedUserRequestFactory.class);
    private final ObjectMapper objectMapper;

    // in testing scenarios and when running the server without the proxy not all the
    // header information will be available. default values will be used in these cases.

    private final ApplicationConfiguration applicationConfiguration;

    @Autowired
    public LocalAuthenticatedUserRequestFactory(ApplicationConfiguration applicationConfiguration) {
        this.applicationConfiguration = applicationConfiguration;
        this.objectMapper = new ObjectMapper();
    }

    // Static method to build an AuthenticatedUserRequest from data available to the controller
    public AuthenticatedUserRequest from(HttpServletRequest servletRequest) {
        HttpServletRequest req = servletRequest;

        Optional<String> token = Optional.ofNullable(req.getHeader("Authorization"))
            .map(header -> header.substring("Bearer ".length()));

        if (!token.isPresent()) {
            return new AuthenticatedUserRequest()
                .email("")
                .token(token)
                .subjectId(applicationConfiguration.getUserId());
        }

        OkHttpClient httpClient = new OkHttpClient.Builder()
            .readTimeout(5, TimeUnit.SECONDS)
            .build();


        String email = Optional.ofNullable(req.getHeader("From"))
            .orElseGet(() -> {
                try {
                    return objectMapper.readValue(httpClient.newCall(new Request.Builder()
                        .url(String.format("https://www.googleapis.com/oauth2/v1/userinfo?access_token=%s", token.get()))
                            .get().build())
                            .execute().body().string(), new TypeReference<Map<String, String>>() {})
                            .get("email");
                } catch (IOException e) {
                    throw new RuntimeException("Failed to reads user info", e);
                }
            });

        String userId = applicationConfiguration.getUserId();

        return new AuthenticatedUserRequest()
            .email(email)
            .subjectId(userId)
            .token(token);
    }
}
