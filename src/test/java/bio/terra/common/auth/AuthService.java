package bio.terra.common.auth;

import bio.terra.common.configuration.TestConfiguration;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.auth.iam.IamProviderInterface;
import com.google.api.client.auth.oauth2.TokenResponseException;
import com.google.api.client.googleapis.auth.oauth2.GoogleCredential;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.javanet.NetHttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import java.io.File;
import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.apache.commons.collections4.map.PassiveExpiringMap.ExpirationPolicy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AuthService {
  private static Logger logger = LoggerFactory.getLogger(AuthService.class);
  private static ExpirationPolicy<String, GoogleCredential> TOKEN_CACHE_EXPIRATION_POLICY =
      // Make sure this value never returns a negative since that means the entry never expires
      (key, value) ->
          Math.max(0, value.getExpirationTimeMilliseconds() - TimeUnit.MINUTES.toMillis(5));

  // the list of scopes we request from end users when they log in.
  // this should always match exactly what the UI requests, so our tests represent actual user
  // behavior:
  private List<String> userLoginScopes =
      List.of(
          "openid", "email", "profile", "https://www.googleapis.com/auth/cloud-billing.readonly");
  private List<String> directAccessScopes =
      List.of(
          "https://www.googleapis.com/auth/bigquery",
          "https://www.googleapis.com/auth/devstorage.full_control");
  private NetHttpTransport httpTransport;
  private JacksonFactory jsonFactory = JacksonFactory.getDefaultInstance();
  private File pemfile;
  private String saEmail;
  private Map<String, GoogleCredential> userTokens =
      Collections.synchronizedMap(new PassiveExpiringMap<>(TOKEN_CACHE_EXPIRATION_POLICY));
  private Map<String, String> petAccountTokens =
      Collections.synchronizedMap(new PassiveExpiringMap<>(55, TimeUnit.MINUTES));
  private Map<String, GoogleCredential> directAccessTokens =
      Collections.synchronizedMap(new PassiveExpiringMap<>(TOKEN_CACHE_EXPIRATION_POLICY));
  private TestConfiguration testConfig;
  private IamProviderInterface iamProvider;

  @Autowired
  public AuthService(TestConfiguration testConfig, IamProviderInterface iamProvider)
      throws Exception {
    this.testConfig = testConfig;
    Optional<String> pemfilename = Optional.ofNullable(testConfig.getJadePemFileName());
    pemfilename.ifPresent(s -> pemfile = new File(s));
    saEmail = testConfig.getJadeEmail();
    httpTransport = GoogleNetHttpTransport.newTrustedTransport();
    this.iamProvider = iamProvider;
  }

  public String getAuthToken(String userEmail) {
    return userTokens.computeIfAbsent(userEmail, this::makeToken).getAccessToken();
  }

  public String getPetAccountAuthToken(String userEmail) {
    return petAccountTokens.computeIfAbsent(userEmail, this::makePetAccountToken);
  }

  public String getDirectAccessAuthToken(String userEmail) {
    return directAccessTokens
        .computeIfAbsent(userEmail, this::makeDirectAccessToken)
        .getAccessToken();
  }

  private GoogleCredential buildCredential(String email, List<String> scopes)
      throws IOException, GeneralSecurityException {
    if (!Optional.ofNullable(pemfile).isPresent()) {
      throw new IllegalStateException(
          String.format("pemfile not found: %s", testConfig.getJadePemFileName()));
    }
    return new GoogleCredential.Builder()
        .setTransport(httpTransport)
        .setJsonFactory(jsonFactory)
        .setServiceAccountId(saEmail)
        .setServiceAccountPrivateKeyFromPemFile(pemfile)
        .setServiceAccountScopes(scopes)
        .setServiceAccountUser(email)
        .build();
  }

  private GoogleCredential makeDirectAccessToken(String userEmail) {
    List<String> allScopes =
        Stream.of(userLoginScopes, directAccessScopes)
            .flatMap(Collection::stream)
            .collect(Collectors.toList());
    return makeTokenForScopes(userEmail, allScopes);
  }

  private GoogleCredential makeTokenForScopes(String userEmail, List<String> scopes) {
    try {
      GoogleCredential cred = buildCredential(userEmail, scopes);
      cred.refreshToken();
      return cred;
    } catch (TokenResponseException e) {
      logger.error("Encountered " + e.getStatusCode() + " error getting access token.");
    } catch (Exception ioe) {
      logger.error("Error getting access token with error message. ", ioe);
    }
    throw new RuntimeException("unable to get access token");
  }

  private GoogleCredential makeToken(String userEmail) {
    return makeTokenForScopes(userEmail, userLoginScopes);
  }

  private String makePetAccountToken(String userEmail) {
    try {
      return iamProvider
          .getPetToken(
              AuthenticatedUserRequest.builder()
                  .setSubjectId("PetServiceAccount")
                  .setEmail(userEmail)
                  .setToken(makeToken(userEmail).getAccessToken())
                  .build(),
              userLoginScopes)
          .replaceAll("\\.+$", "");
    } catch (InterruptedException e) {
      throw new RuntimeException(e);
    }
  }
}
