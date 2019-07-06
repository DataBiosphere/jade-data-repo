package bio.terra.integration.auth;

import bio.terra.integration.configuration.TestConfiguration;
import com.google.api.client.auth.oauth2.TokenResponseException;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Profile;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.*;
import java.util.stream.Collectors;
import java.util.stream.Stream;

@Component
@Profile("integrationtest")
public class AuthService {
    private static Logger logger = LoggerFactory.getLogger(AuthService.class);
    // the list of scopes we request from end users when they log in.
    // this should always match exactly what the UI requests, so our tests represent actual user behavior:
    private List<String> userLoginScopes = Arrays.asList("openid", "email", "profile");
    private List<String> directAccessScopes = Arrays.asList(
        "https://www.googleapis.com/auth/bigquery",
        "https://www.googleapis.com/auth/devstorage.full_control");
    private NetHttpTransport httpTransport;
    private JacksonFactory jsonFactory = JacksonFactory.getDefaultInstance();
    private File pemfile;
    private String saEmail;
    private Map<String, String> userTokens = new HashMap<>();
    private Map<String, String> directAccessTokens = new HashMap<>();

    @Autowired
    public AuthService(TestConfiguration testConfig) throws Exception {
        String pemfilename = testConfig.getJadePemFileName();
        pemfile = new File(pemfilename);
        saEmail = testConfig.getJadeEmail();
        httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    }

    public String getAuthToken(String userEmail) {
        if (!userTokens.containsKey(userEmail)) {
            userTokens.put(userEmail, makeToken(userEmail));
        }
        return userTokens.get(userEmail);
    }

    public String getDirectAccessAuthToken(String userEmail) {
        if (!directAccessTokens.containsKey(userEmail)) {
            directAccessTokens.put(userEmail, makeDirectAccessToken(userEmail));
        }
        return directAccessTokens.get(userEmail);
    }

    private GoogleCredential buildCredential(String email, List<String> scopes) throws IOException, GeneralSecurityException {
        return new GoogleCredential.Builder()
            .setTransport(httpTransport)
            .setJsonFactory(jsonFactory)
            .setServiceAccountId(saEmail)
            .setServiceAccountPrivateKeyFromPemFile(pemfile)
            .setServiceAccountScopes(scopes)
            .setServiceAccountUser(email)
            .build();
    }

    private String makeDirectAccessToken(String userEmail) {
        List<String> allScopes = Stream.of(
            userLoginScopes,
            directAccessScopes)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
        return makeTokenForScopes(userEmail, allScopes);
    }

    private String makeTokenForScopes(String userEmail, List<String> scopes) {
        try {
            GoogleCredential cred = buildCredential(userEmail, scopes);
            cred.refreshToken();
            return cred.getAccessToken();
        } catch (TokenResponseException e) {
            logger.error("Encountered " + e.getStatusCode() + " error getting access token.");
        } catch (Exception ioe) {
            logger.error("Error getting access token with error message. ", ioe);
        }
        throw new RuntimeException("unable to get access token");
    }

    private String makeToken(String userEmail) {
        return makeTokenForScopes(userEmail, userLoginScopes);
    }

}

