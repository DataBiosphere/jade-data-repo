package bio.terra.app.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "ct")
public class ConnectedTestConfiguration {

    private String ingestbucket;

    public String getIngestbucket() {
        return ingestbucket;
    }

    public void setIngestbucket(String ingestbucket) {
        this.ingestbucket = ingestbucket;
    }
}
