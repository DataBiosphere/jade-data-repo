package bio.terra.app.configuration;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ServiceAccountCredentials;
import java.io.FileInputStream;
import java.io.IOException;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

/** Configuration for managing connection to Buffer Service. * */
@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "rbs")
public class ResourceBufferServiceConfiguration {

  private boolean enabled;
  private String instanceUrl;
  private String poolId;
  private String clientCredentialFilePath;

  // I think we'd want to re-use our app scopes.
  private static final List<String> BUFFER_SCOPES = List.of("openid", "email", "profile");

  public boolean getEnabled() {
    return enabled;
  }

  public void setEnabled(boolean enabled) {
    this.enabled = enabled;
  }

  public String getInstanceUrl() {
    return instanceUrl;
  }

  public void setInstanceUrl(String instanceUrl) {
    this.instanceUrl = instanceUrl;
  }

  public String getPoolId() {
    return poolId;
  }

  public void setPoolId(String poolId) {
    this.poolId = poolId;
  }

  public String getClientCredentialFilePath() {
    return clientCredentialFilePath;
  }

  public void setClientCredentialFilePath(String clientCredentialFilePath) {
    this.clientCredentialFilePath = clientCredentialFilePath;
  }

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
