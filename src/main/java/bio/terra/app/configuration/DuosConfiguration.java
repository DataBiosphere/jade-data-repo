package bio.terra.app.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@ConfigurationProperties(prefix = "duos")
public class DuosConfiguration {

  private String basePath;

  public String getBasePath() {
    return basePath;
  }

  public void setBasePath(String basePath) {
    this.basePath = basePath;
  }
}
