package bio.terra.service.resourcemanagement.azure;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.jupiter.api.Assertions.*;

import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;
import org.stringtemplate.v4.ST;

@ActiveProfiles({"google", "unittest"})
@Tag("bio.terra.common.category.Unit")
class AzureStorageAccountResourceTest {
  @Test
  void testGetStorageAccountResourceId() {
    UUID subscriptionId = UUID.fromString("deadbeef-face-cafe-bead-0ddba11deed5");
    String resourceId =
        new ST(
                "/subscriptions/<subscriptionId>/resourceGroups/<resourceGroup>/providers/microsoft.solutions/applications/<appName>")
            .add("subscriptionId", subscriptionId)
            .add("resourceGroup", "TDR")
            .add("appName", "myapp")
            .render();
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
