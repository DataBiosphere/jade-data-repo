package bio.terra.service.auth.oauth2;

import bio.terra.common.exception.ServiceInitializationException;
import bio.terra.service.auth.oauth2.exceptions.OauthTokeninfoRetrieveException;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.services.oauth2.Oauth2;
import com.google.api.services.oauth2.model.Tokeninfo;
import java.io.IOException;
import java.security.GeneralSecurityException;

public class GoogleOauthUtils {
  public static final String APPLICATION_NAME = "Terra Data Repository";
  private static final HttpTransport HTTP_TRANSPORT;

  static {
    try {
      HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
    } catch (IOException | GeneralSecurityException e) {
      throw new ServiceInitializationException("Could not build HTTP Transport", e);
    }
  }

  private static final JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();

  /**
   * Call the Google tokeninfo endpoint to retrieve basic information about the token. Note: this
   * does not return the actual token
   *
   * @param accessToken Valid Oauth access token
   * @return Basic information about the token
   */
  public static Tokeninfo getOauth2TokenInfo(String accessToken) {
    Oauth2 oauth2 =
        new Oauth2.Builder(HTTP_TRANSPORT, JSON_FACTORY, null)
            .setApplicationName(APPLICATION_NAME)
            .build();
    try {
      return oauth2.tokeninfo().setAccessToken(accessToken).execute();
    } catch (IOException e) {
      throw new OauthTokeninfoRetrieveException("Error retrieving token information", e);
    }
  }
}
