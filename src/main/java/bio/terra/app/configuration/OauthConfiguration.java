package bio.terra.app.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "oauth")
public class OauthConfiguration {

  private String schemeName;
  private String[] scopes;
  private String loginEndpoint;
  private String clientId;
  private String clientSecret;

  public String getSchemeName() {
    return schemeName;
  }

  public void setSchemeName(String schemeName) {
    this.schemeName = schemeName;
  }

  public String getLoginEndpoint() {
    return loginEndpoint;
  }

  public void setLoginEndpoint(String loginEndpoint) {
    this.loginEndpoint = loginEndpoint;
  }

  public String getClientId() {
    return clientId;
  }

  public void setClientId(String clientId) {
    this.clientId = clientId;
  }

  public String getClientSecret() {
    return clientSecret;
  }

  public void setClientSecret(String clientSecret) {
    this.clientSecret = clientSecret;
  }

  public String[] getScopes() {
    return scopes.clone();
  }

  public void setScopes(String[] scopes) {
    this.scopes = scopes.clone();
  }
}
