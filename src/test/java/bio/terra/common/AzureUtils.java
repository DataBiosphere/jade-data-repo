package bio.terra.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.configuration.TestConfiguration;
import bio.terra.service.filedata.google.firestore.FireStoreDependency;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import com.azure.data.tables.models.TableEntity;
import com.azure.resourcemanager.AzureResourceManager;
import java.util.UUID;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class AzureUtils {

  @Autowired AzureResourceConfiguration azureResourceConfiguration;
  @Autowired ConnectedTestConfiguration connectedTestConfiguration;
  @Autowired TestConfiguration testConfiguration;

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
        connectedTestConfiguration.getTargetTenantId(),
        connectedTestConfiguration.getTargetSubscriptionId(),
        connectedTestConfiguration.getTargetResourceGroupName(),
        connectedTestConfiguration.getSourceStorageAccountName());
  }

  public void assertEntityCorrect(
      TableEntity entity, UUID snapshotId, String fileId, Long refCount) {
    assertThat(
        snapshotId.toString(),
        equalTo(entity.getProperty(FireStoreDependency.SNAPSHOT_ID_FIELD_NAME)));
    assertThat(fileId, equalTo(entity.getProperty(FireStoreDependency.FILE_ID_FIELD_NAME)));
    assertThat(refCount, equalTo(entity.getProperty(FireStoreDependency.REF_COUNT_FIELD_NAME)));
  }
}
