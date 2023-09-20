package bio.terra.service.resourcemanagement.azure;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.service.resourcemanagement.MetadataDataAccessUtils;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@Tag("bio.terra.common.category.Unit")
class AzureStorageAccountResourceTest {
  @Test
  void testGetStorageAccountResourceId() {
    UUID subscriptionId = UUID.fromString("deadbeef-face-cafe-bead-0ddba11deed5");
    String resourceId =
        MetadataDataAccessUtils.getApplicationDeploymentId(subscriptionId, "TDR", "myapp");
    AzureApplicationDeploymentResource appResource =
        new AzureApplicationDeploymentResource()
            .azureApplicationDeploymentId(resourceId)
            .azureResourceGroupName("mrg");

    AzureStorageAccountResource resource =
        new AzureStorageAccountResource().applicationResource(appResource).name("storagename");

    assertThat(
        "storage account resource id can be generated",
        resource.getStorageAccountId(),
        is(
            "/subscriptions/deadbeef-face-cafe-bead-0ddba11deed5/resourceGroups/mrg/providers/Microsoft.Storage/storageAccounts/storagename"));
  }
}
