package bio.terra.common.auth;

import bio.terra.common.configuration.TestConfiguration;
import com.google.api.client.auth.oauth2.TokenResponseException;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
public class AuthService {
    private static Logger logger = LoggerFactory.getLogger(AuthService.class);
    // the list of scopes we request from end users when they log in.
    // this should always match exactly what the UI requests, so our tests represent actual user behavior:
    private List<String> userLoginScopes = Arrays.asList("openid", "email", "profile");
    private List<String> directAccessScopes = Arrays.asList(
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/devstorage.full_control");
    private File pemfile;
    private String saEmail;
    private TestConfiguration testConfig;

    private Map<String, GoogleCredentials> userCreds = new HashMap<>();
    private Map<String, GoogleCredentials> directAccessCreds = new HashMap<>();

    @Autowired
    public AuthService(TestConfiguration testConfig) throws Exception {
        this.testConfig = testConfig;
        Optional<String> pemfilename = Optional.ofNullable(testConfig.getJadePemFileName());
        pemfilename.ifPresent(s -> pemfile = new File(s));
        saEmail = testConfig.getJadeEmail();
    }

    public String getAuthToken(String userEmail) {
        return getCredentials(userEmail).getAccessToken().getTokenValue();
    }

    public String getDirectAccessAuthToken(String userEmail) {
        if (!directAccessCreds.containsKey(userEmail)) {
            directAccessCreds.put(userEmail, makeDirectAccessCredentials(userEmail));
        }
        return directAccessCreds.get(userEmail).getAccessToken().getTokenValue();
    }

    public GoogleCredentials getCredentials(String userEmail) {
        if (!userCreds.containsKey(userEmail)) {
            userCreds.put(userEmail, makeToken(userEmail));
        }
        return userCreds.get(userEmail);
    }

    private GoogleCredentials buildCredentials(String email, List<String> scopes) throws IOException {
        if (!Optional.ofNullable(pemfile).isPresent()) {
            throw new IllegalStateException(String.format("pemfile not found: %s", testConfig.getJadePemFileName()));
        }
        return ServiceAccountCredentials.fromStream(new FileInputStream(pemfile))
            .createDelegated(email)
            .createScoped(scopes);
    }

    private GoogleCredentials makeDirectAccessCredentials(String userEmail) {
        List<String> allScopes = Stream.of(
            userLoginScopes,
            directAccessScopes)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());

        return makeCredentialsForScopes(userEmail, allScopes);
    }

    private GoogleCredentials makeCredentialsForScopes(String userEmail, List<String> scopes) {
        try {
            GoogleCredentials creds = buildCredentials(userEmail, scopes);
            creds.refresh();
            return creds;
        } catch (TokenResponseException e) {
            logger.error("Encountered " + e.getStatusCode() + " error getting access token.");
        } catch (Exception ioe) {
            logger.error("Error getting access token with error message. ", ioe);
        }
        throw new RuntimeException("unable to get access token");
    }

    private GoogleCredentials makeToken(String userEmail) {
        return makeCredentialsForScopes(userEmail, userLoginScopes);
    }

}

