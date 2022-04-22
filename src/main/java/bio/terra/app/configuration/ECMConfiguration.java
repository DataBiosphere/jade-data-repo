package bio.terra.app.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "ecm")
public class ECMConfiguration {

  private String basePath;
  private String rasIssuer;

  public String getBasePath() {
    return basePath;
  }

  public void setBasePath(String basePath) {
    this.basePath = basePath;
  }

  public String getRasIssuer() {
    return rasIssuer;
  }

  public void setRasIssuer(String rasIssuer) {
    this.rasIssuer = rasIssuer;
  }
}
