package bio.terra.app.configuration;

import com.google.auth.oauth2.AccessToken;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.auth.oauth2.ImpersonatedCredentials;
import java.io.IOException;
import java.util.List;
import org.springframework.boot.context.properties.ConfigurationProperties;

/** Configuration for managing connection to Buffer Service. * */
@ConfigurationProperties(prefix = "rbs")
public record ResourceBufferServiceConfiguration(
    boolean enabled, String instanceUrl, String poolId, String clientCredentialFilePath) {

  // I think we'd want to re-use our app scopes.
  private static final List<String> BUFFER_SCOPES = List.of("openid", "email", "profile");

  public String getAccessToken() throws IOException {
    GoogleCredentials sourceCredentials = GoogleCredentials.getApplicationDefault();

    ImpersonatedCredentials targetCredentials =
        ImpersonatedCredentials.create(
            sourceCredentials,
            "buffer-tools@terra-kernel-k8s.iam.gserviceaccount.com",
            null,
            BUFFER_SCOPES,
            3600);

    var targetCredentialsScoped = targetCredentials.createScoped(BUFFER_SCOPES);
    return targetCredentialsScoped.refreshAccessToken().getTokenValue();
  }
}
