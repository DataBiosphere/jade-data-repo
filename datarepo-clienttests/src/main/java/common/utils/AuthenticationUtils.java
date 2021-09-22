package common.utils;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import runner.config.ServiceAccountSpecification;
import runner.config.TestUserSpecification;

public final class AuthenticationUtils {
  private static volatile GoogleCredentials applicationDefaultCredential;
  private static volatile GoogleCredentials serviceAccountCredential;
  private static Map<String, GoogleCredentials> delegatedUserCredentials =
      new ConcurrentHashMap<>();

  private static final Object lockApplicationDefaultCredential = new Object();
  private static final Object lockServiceAccountCredential = new Object();

  private AuthenticationUtils() {}

  // the list of scopes we request from end users when they log in. this should always match exactly
  // what the UI requests, so our tests represent actual user behavior
  private static final List<String> userLoginScopes =
      Arrays.asList(
          "openid", "email", "profile", "https://www.googleapis.com/auth/cloud-billing.readonly");

  // the list of "extra" scopes we request for the test users, so that we can access BigQuery and
  // Cloud Storage directly (e.g. to query the snapshot table, write a file to a scratch bucket)
  private static final List<String> directAccessScopes =
      Arrays.asList(
          "https://www.googleapis.com/auth/bigquery",
          "https://www.googleapis.com/auth/devstorage.full_control");

  public static GoogleCredentials getDelegatedUserCredential(
      TestUserSpecification testUserSpecification) throws IOException {
    GoogleCredentials delegatedUserCredential =
        delegatedUserCredentials.get(testUserSpecification.userEmail);
    if (delegatedUserCredential != null) {
      return delegatedUserCredential;
    }

    List<String> scopes = new ArrayList<>();
    scopes.addAll(userLoginScopes);
    scopes.addAll(directAccessScopes);

    GoogleCredentials serviceAccountCredential =
        getServiceAccountCredential(testUserSpecification.delegatorServiceAccount);
    delegatedUserCredential =
        serviceAccountCredential
            .createScoped(scopes)
            .createDelegated(testUserSpecification.userEmail);
    delegatedUserCredentials.put(testUserSpecification.userEmail, delegatedUserCredential);
    return delegatedUserCredential;
  }

  public static GoogleCredentials getServiceAccountCredential(
      ServiceAccountSpecification serviceAccount) throws IOException {
    if (serviceAccountCredential != null) {
      return serviceAccountCredential;
    }

    synchronized (lockServiceAccountCredential) {
      File jsonKey = serviceAccount.jsonKeyFile;
      serviceAccountCredential =
          ServiceAccountCredentials.fromStream(new FileInputStream(jsonKey))
              .createScoped(
                  Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
    }
    return serviceAccountCredential;
  }

  public static GoogleCredentials getApplicationDefaultCredential() throws IOException {
    if (applicationDefaultCredential != null) {
      return applicationDefaultCredential;
    }

    synchronized (lockApplicationDefaultCredential) {
      applicationDefaultCredential =
          GoogleCredentials.getApplicationDefault()
              .createScoped(
                  Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
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
