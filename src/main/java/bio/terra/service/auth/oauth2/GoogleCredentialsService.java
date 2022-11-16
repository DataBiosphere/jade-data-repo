package bio.terra.service.auth.oauth2;

import bio.terra.service.resourcemanagement.exception.GoogleResourceException;
import com.google.auth.oauth2.GoogleCredentials;
import java.io.IOException;
import java.util.List;
import org.springframework.stereotype.Component;

@Component
public class GoogleCredentialsService {

  private List<String> scopes = List.of();

  public List<String> getScopes() {
    return this.scopes;
  }

  public GoogleCredentialsService scopes(List<String> scopes) {
    this.scopes = scopes;
    return this;
  }

  /**
   * @return the application's default credentials
   */
  public GoogleCredentials getApplicationDefault() {
    try {
      return GoogleCredentials.getApplicationDefault();
    } catch (IOException e) {
      throw new GoogleResourceException("Could not generate Google credentials", e);
    }
  }

  /**
   * @return valid oauth token for credentials with scopes specified when required
   */
  public String getAccessToken(GoogleCredentials credentials) {
    try {
      if (credentials.createScopedRequired()) {
        credentials = credentials.createScoped(scopes);
      }
      return credentials.refreshAccessToken().getTokenValue();
    } catch (IOException e) {
      throw new GoogleResourceException("Could not generate Google access token", e);
    }
  }

  /**
   * @return valid oauth token for application's default credentials with scopes specified when
   *     required
   */
  public String getApplicationDefaultAccessToken() {
    GoogleCredentials credentials = getApplicationDefault();
    return getAccessToken(credentials);
  }
}
