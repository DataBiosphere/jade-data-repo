package bio.terra.app.configuration;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.ConstructorBinding;

/** Configuration for managing connection to Buffer Service. * */
@ConfigurationProperties(prefix = "rbs")
@ConstructorBinding
public record ResourceBufferServiceConfiguration(
    boolean enabled, String instanceUrl, String poolId, String clientCredentialFilePath) {

  // I think we'd want to re-use our app scopes.
  private static final List<String> BUFFER_SCOPES = List.of("openid", "email", "profile");

  // TODO - not sure if this is actually how we want to do this, just copying wsm's implementation
  // for now
  public String getAccessToken() throws IOException {
    try (FileInputStream fileInputStream = new FileInputStream(clientCredentialFilePath)) {
      GoogleCredentials credentials =
          ServiceAccountCredentials.fromStream(fileInputStream).createScoped(BUFFER_SCOPES);
      AccessToken token = credentials.refreshAccessToken();
      return token.getTokenValue();
    }
  }
}
