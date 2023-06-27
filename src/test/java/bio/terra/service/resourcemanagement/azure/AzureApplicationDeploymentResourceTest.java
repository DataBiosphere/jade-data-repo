package bio.terra.service.resourcemanagement.azure;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;
import org.stringtemplate.v4.ST;

@ActiveProfiles({"google", "unittest"})
@Tag("bio.terra.common.category.Unit")
class AzureApplicationDeploymentResourceTest {

  @Test
  void testExtractSubscriptionFromResourceId() {
    UUID subscriptionId = UUID.randomUUID();

    String resourceId =
        new ST(
                "/subscriptions/<subscriptionId>/resourceGroups/<resourceGroup>/providers/microsoft.solutions/applications/<appName>")
            .add("subscriptionId", subscriptionId)
            .add("resourceGroup", "TDR")
            .add("appName", "myapp")
            .render();

    AzureApplicationDeploymentResource resource =
        new AzureApplicationDeploymentResource().azureApplicationDeploymentId(resourceId);
    assertThat("subscription can be extracted", resource.getSubscriptionId(), is(subscriptionId));
  }
}
