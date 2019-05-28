package bio.terra.auth;

import bio.terra.integration.configuration.TestConfiguration;
import com.google.api.client.auth.oauth2.TokenResponseException;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Arrays;
import java.util.List;

@Component
public class AuthService {
    private static Logger logger = LoggerFactory.getLogger(AuthService.class);
    // the list of scopes we request from end users when they log in.
    // this should always match exactly what the UI requests, so our tests represent actual user behavior:
    private List<String> userLoginScopes = Arrays.asList(new String[]{"openid", "email", "profile"});

//    private TestConfiguration testConfig;
    private NetHttpTransport httpTransport;
    private JacksonFactory jsonFactory = JacksonFactory.getDefaultInstance();

    private File pemfile;
    private String saEmail;

    @Autowired
    public AuthService(TestConfiguration testConfig) throws Exception {
        pemfile = new File(testConfig.getJadePemFile());
        saEmail = testConfig.getJadeEmail();
        httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    }

    public String getAuthToken(Credentials userCred) {
        return makeToken(userCred);
    }


    private GoogleCredential buildCredential(String email) throws IOException, GeneralSecurityException {
        return new GoogleCredential.Builder()
            .setTransport(httpTransport)
            .setJsonFactory(jsonFactory)
            .setServiceAccountId(saEmail)
            .setServiceAccountPrivateKeyFromPemFile(pemfile)
            .setServiceAccountScopes(userLoginScopes)
            .setServiceAccountUser(email)
            .build();
    }


    private String makeToken(Credentials userCred) {
        try {
            GoogleCredential cred = buildCredential(userCred.getEmail());
            cred.refreshToken();
            return cred.getAccessToken();
        } catch (TokenResponseException e) {
            logger.error("Encountered " + e.getStatusCode() + " error getting access token.");
        } catch (Exception ioe) {
            logger.error("Error getting access token with error message. ", ioe);
        }
        throw new RuntimeException("unable to get access token");
    }

}

