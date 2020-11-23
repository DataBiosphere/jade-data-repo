package bio.terra.app.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

@Component
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "ct")
public class ConnectedTestConfiguration {

    private String ingestbucket;
    private String ingestRequesterPaysBucket;
    private String googleBillingAccountId;
    private String secondGoogleBillingAccountId;

    public String getIngestbucket() {
        return ingestbucket;
    }

    public void setIngestbucket(String ingestbucket) {
        this.ingestbucket = ingestbucket;
    }

    public String getIngestRequesterPaysBucket() {
        return ingestRequesterPaysBucket;
    }

    public void setIngestRequesterPaysBucket(String ingestRequesterPaysBucket) {
        this.ingestRequesterPaysBucket = ingestRequesterPaysBucket;
    }

    public String getGoogleBillingAccountId() {
        return googleBillingAccountId;
    }

    public void setGoogleBillingAccountId(String googleBillingAccountId) {
        this.googleBillingAccountId = googleBillingAccountId;
    }

    // TODO- IS THERE A BETTER NAME FOR THIS?
    public String getSecondGoogleBillingAccountId() {
        return secondGoogleBillingAccountId;
    }

    public void setSecondGoogleBillingAccountId(String secondGoogleBillingAccountId) {
        this.secondGoogleBillingAccountId = secondGoogleBillingAccountId;
    }
}
