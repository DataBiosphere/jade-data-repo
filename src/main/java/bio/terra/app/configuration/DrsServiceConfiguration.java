package bio.terra.app.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "drs")
public class DrsServiceConfiguration {

  private String rasIssuer;

  public String getRasIssuer() {
    return rasIssuer;
  }

  public void setRasIssuer(String rasIssuer) {
    this.rasIssuer = rasIssuer;
  }
}
