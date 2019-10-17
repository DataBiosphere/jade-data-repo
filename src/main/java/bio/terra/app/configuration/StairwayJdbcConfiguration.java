package bio.terra.app.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "db.stairway")
public class StairwayJdbcConfiguration extends JdbcConfiguration {
    private String forceClean;

    public String getForceClean() {
        return forceClean;
    }

    public void setForceClean(String forceClean) {
        this.forceClean = forceClean;
    }

    public boolean isForceClean() {
        return Boolean.parseBoolean(forceClean);
    }
}
