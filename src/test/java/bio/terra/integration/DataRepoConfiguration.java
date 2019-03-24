package bio.terra.integration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "integrationtest")
public class DataRepoConfiguration {
    private String port;
    private String server;
    private String protocol;
    private String ingestbucket;

    public String getPort() {
        return port;
    }

    public String getServer() {
        return server;
    }

    public String getProtocol() {
        return protocol;
    }

    public String getIngestbucket() {
        return ingestbucket;
    }

    public void setPort(String port) {
        this.port = port;
    }

    public void setServer(String server) {
        this.server = server;
    }

    public void setProtocol(String protocol) {
        this.protocol = protocol;
    }

    public void setIngestbucket(String ingestbucket) {
        this.ingestbucket = ingestbucket;
    }
}
