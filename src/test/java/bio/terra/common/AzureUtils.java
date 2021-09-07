package bio.terra.common;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import com.azure.resourcemanager.AzureResourceManager;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AzureUtils {

  @Autowired AzureResourceConfiguration azureResourceConfiguration;
  @Autowired ConnectedTestConfiguration testConfiguration;

  public String getSourceStorageAccountPrimarySharedKey(
      UUID targetTenantId,
      UUID targetSubscriptionId,
      String targetResourceGroupName,
      String sourceStorageAccountName) {
    AzureResourceManager client =
        azureResourceConfiguration.getClient(targetTenantId, targetSubscriptionId);
    return client
        .storageAccounts()
        .getByResourceGroup(targetResourceGroupName, sourceStorageAccountName)
        .getKeys()
        .iterator()
        .next()
        .value();
  }

  public String getSourceStorageAccountPrimarySharedKey() {
    return getSourceStorageAccountPrimarySharedKey(
        testConfiguration.getTargetTenantId(),
        testConfiguration.getTargetSubscriptionId(),
        testConfiguration.getTargetResourceGroupName(),
        testConfiguration.getSourceStorageAccountName());
  }
}
