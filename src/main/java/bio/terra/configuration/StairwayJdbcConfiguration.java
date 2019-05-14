package bio.terra.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "db.stairway")
public class StairwayJdbcConfiguration extends JdbcConfiguration {
    private String stairwayForceClean;

    public String getStairwayForceClean() {
        return stairwayForceClean;
    }

    public void setStairwayForceClean(String stairwayForceClean) {
        this.stairwayForceClean = stairwayForceClean;
    }
}
