package bio.terra.app.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "sam")
public class SamConfiguration {
    private String basePath;
    private String stewardsGroupEmail;

    public String getBasePath() {
        return basePath;
    }

    public void setBasePath(String basePath) {
        this.basePath = basePath;
    }

    public String getStewardsGroupEmail() {
        return stewardsGroupEmail;
    }

    public void setStewardsGroupEmail(String stewardsGroupEmail) {
        this.stewardsGroupEmail = stewardsGroupEmail;
    }
}
