package bio.terra.service.resourcemanagement.azure;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.AzureUtils;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@Tag("bio.terra.common.category.Unit")
class AzureApplicationDeploymentResourceTest {

  @Test
  void testExtractSubscriptionFromResourceId() {
    UUID subscriptionId = UUID.randomUUID();

    String resourceId =
        AzureUtils.getApplicationDeploymentResourceId(subscriptionId, "TDR", "myapp");

    AzureApplicationDeploymentResource resource =
        new AzureApplicationDeploymentResource().azureApplicationDeploymentId(resourceId);
    assertThat("subscription can be extracted", resource.getSubscriptionId(), is(subscriptionId));
  }
}
