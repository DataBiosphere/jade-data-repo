package utils;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import runner.config.ServiceAccountSpecification;
import runner.config.TestUserSpecification;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public final class AuthenticationUtils {

    private static GoogleCredentials applicationDefaultCredential;
    private static ServiceAccountCredentials serviceAccountCredential;
    private static Map<String, GoogleCredentials> delegatedUserCredentials = new ConcurrentHashMap<>();

    private AuthenticationUtils() { }

    public static GoogleCredentials getDelegatedUserCredential(
        TestUserSpecification testUserSpecification, List<String> scopes) throws IOException {
        GoogleCredentials delegatedUserCredential = delegatedUserCredentials.get(testUserSpecification.userEmail);
        if (delegatedUserCredential == null) {
            ServiceAccountCredentials serviceAccountCredential =
                getServiceAccountCredential(testUserSpecification.delegatorServiceAccount);
            delegatedUserCredential = serviceAccountCredential
                .createScoped(scopes).createDelegated(testUserSpecification.userEmail);
            delegatedUserCredentials.put(testUserSpecification.userEmail, delegatedUserCredential);
        }
        return delegatedUserCredential;
    }

    public static ServiceAccountCredentials getServiceAccountCredential(ServiceAccountSpecification serviceAccount)
        throws IOException {
        if (serviceAccountCredential == null) {
            File jsonKey = serviceAccount.jsonKeyFile;
            serviceAccountCredential = ServiceAccountCredentials
                .fromStream(new FileInputStream(jsonKey));
        }
        return serviceAccountCredential;
    }

    public static GoogleCredentials getApplicationDefaultCredential() throws IOException {
        if (applicationDefaultCredential == null) {
            applicationDefaultCredential = GoogleCredentials.getApplicationDefault();
            if (applicationDefaultCredential.createScopedRequired()) { // when is this true?
                applicationDefaultCredential = applicationDefaultCredential.createScoped(
                    Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
            }
        }
        return applicationDefaultCredential;
    }

    public static AccessToken getAccessToken(GoogleCredentials credential) {
        try {
            credential.refreshIfExpired();
            return credential.getAccessToken();
        } catch (IOException ioEx) {
            throw new RuntimeException("Error refreshing access token", ioEx);
        }
    }
}
