package bio.terra.app.configuration;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import java.io.IOException;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/** Configuration for managing connection to Terra Policy Service. * */
@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "tps")
public class PolicyServiceConfiguration {

  private boolean enabled;
  private String basePath;

  private static final List<String> POLICY_SCOPES = List.of("openid", "email", "profile");

  public boolean getEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public String getBasePath() {
    return basePath;
  }

  public void setBasePath(String basePath) {
    this.basePath = basePath;
  }

  public String getAccessToken() throws IOException {
    GoogleCredentials credentials =
        ServiceAccountCredentials.getApplicationDefault().createScoped(POLICY_SCOPES);
    AccessToken token = credentials.refreshAccessToken();
    return token.getTokenValue();
  }
}
