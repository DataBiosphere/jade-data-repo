package bio.terra.common.configuration;

import java.util.List;
import java.util.UUID;
import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "it")
public record TestConfiguration(
    String jadeApiUrl,
    String jadePemFileName,
    String jadeEmail,
    String ingestbucket,
    List<User> users,
    String googleProjectId,
    String googleBillingAccountId,
    UUID targetTenantId,
    UUID targetSubscriptionId,
    String targetResourceGroupName,
    String targetManagedResourceGroupName,
    String targetApplicationName,
    String sourceStorageAccountName,
    String ingestRequestContainer) {

  public record User(String role, String name, String email) {}
}
