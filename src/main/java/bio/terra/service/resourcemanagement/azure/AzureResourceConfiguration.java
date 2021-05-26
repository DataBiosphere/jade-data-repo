package bio.terra.service.resourcemanagement.azure;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import java.util.UUID;

/**
 * Configuration for working with Azure resources
 */
@Configuration
@EnableConfigurationProperties
@ConfigurationProperties(prefix = "azure")
public class AzureResourceConfiguration {
    private Credentials credentials;

    public Credentials getCredentials() {
        return credentials;
    }

    public void setCredentials(Credentials credentials) {
        this.credentials = credentials;
    }

    /**
     * Information for authenticating the TDR service against user Azure tenants
     */
    public static class Credentials {
        // The unique UUID of the TDR application
        private UUID applicationId;
        // A valid and current secret (e.g. application password) for the TDR application
        private String secret;
        // The UUID of the tenant to which the application belongs
        private UUID homeTenantId;

        public UUID getApplicationId() {
            return applicationId;
        }

        public void setApplicationId(UUID applicationId) {
            this.applicationId = applicationId;
        }

        public String getSecret() {
            return secret;
        }

        public void setSecret(String secret) {
            this.secret = secret;
        }

        public UUID getHomeTenantId() {
            return homeTenantId;
        }

        public void setHomeTenantId(UUID homeTenantId) {
            this.homeTenantId = homeTenantId;
        }
    }
}
