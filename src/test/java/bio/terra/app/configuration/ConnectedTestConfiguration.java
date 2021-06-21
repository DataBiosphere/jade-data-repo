package bio.terra.app.configuration;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

import java.util.UUID;

@Component
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "ct")
public class ConnectedTestConfiguration {

    private String ingestbucket;
    private String nonDefaultRegionIngestBucket;
    private String ingestRequesterPaysBucket;
    private String googleBillingAccountId;
    private String noSpendGoogleBillingAccountId;
    private UUID targetTenantId;
    private UUID targetSubscriptionId;
    private String targetResourceGroupName;
    private String targetApplicationName;

    public String getIngestbucket() {
        return ingestbucket;
    }

    public String getNonDefaultRegionIngestBucket() {
        return nonDefaultRegionIngestBucket;
    }

    public void setNonDefaultRegionIngestBucket(String nonDefaultRegionIngestBucket) {
        this.nonDefaultRegionIngestBucket = nonDefaultRegionIngestBucket;
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

    public String getNoSpendGoogleBillingAccountId() {
        return noSpendGoogleBillingAccountId;
    }

    public void setNoSpendGoogleBillingAccountId(String secondGoogleBillingAccountId) {
        this.noSpendGoogleBillingAccountId = secondGoogleBillingAccountId;
    }

    public UUID getTargetTenantId() {
        return targetTenantId;
    }

    public void setTargetTenantId(UUID targetTenantId) {
        this.targetTenantId = targetTenantId;
    }

    public UUID getTargetSubscriptionId() {
        return targetSubscriptionId;
    }

    public void setTargetApplicationName(String targetApplicationName) {
        this.targetApplicationName = targetApplicationName;
    }

    public String getTargetApplicationName() {
        return targetApplicationName;
    }

    public void setTargetSubscriptionId(UUID targetSubscriptionId) {
        this.targetSubscriptionId = targetSubscriptionId;
    }

    public String getTargetResourceGroupName() {
        return targetResourceGroupName;
    }

    public void setTargetResourceGroupName(String targetResourceGroupName) {
        this.targetResourceGroupName = targetResourceGroupName;
    }
}
