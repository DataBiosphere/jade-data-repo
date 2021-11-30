package common.utils;

import com.azure.core.credential.TokenCredential;
import com.azure.core.management.AzureEnvironment;
import com.azure.core.management.profile.AzureProfile;
import com.azure.identity.ClientSecretCredentialBuilder;
import com.azure.resourcemanager.AzureResourceManager;
import java.util.UUID;

public class AzureAuthUtils {

  public static AzureResourceManager getClient(final UUID tenantId, final UUID subscriptionId) {
    final AzureProfile profile =
        new AzureProfile(tenantId.toString(), subscriptionId.toString(), AzureEnvironment.AZURE);
    return AzureResourceManager.authenticate(getAppToken(tenantId), profile)
        .withSubscription(subscriptionId.toString());
  }

  public static TokenCredential getAppToken(final UUID tenantId) {
    String APPLICATION_ID = System.getenv("AZURE_CREDENTIALS_APPLICATIONID");
    String SECRET = System.getenv("AZURE_CREDENTIALS_SECRET");
    return new ClientSecretCredentialBuilder()
        .clientId(APPLICATION_ID)
        .clientSecret(SECRET)
        .tenantId(tenantId.toString())
        .build();
  }
}
