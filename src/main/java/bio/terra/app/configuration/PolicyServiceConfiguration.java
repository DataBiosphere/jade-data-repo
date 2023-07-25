package bio.terra.app.configuration;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import java.io.IOException;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Configuration for managing connection to Terra Policy Service. * */
@ConfigurationProperties(prefix = "tps")
public record PolicyServiceConfiguration(boolean enabled, String basePath) {

  private static final List<String> POLICY_SCOPES = List.of("openid", "email", "profile");

  public String getAccessToken() throws IOException {
    GoogleCredentials credentials =
        ServiceAccountCredentials.getApplicationDefault().createScoped(POLICY_SCOPES);
    AccessToken token = credentials.refreshAccessToken();
    return token.getTokenValue();
  }
}
