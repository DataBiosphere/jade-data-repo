package bio.terra.datarepo.common.fixtures;

import bio.terra.datarepo.app.model.AzureRegion;
import bio.terra.datarepo.app.model.AzureStorageAccountSkuType;
import bio.terra.datarepo.model.BillingProfileModel;
import bio.terra.datarepo.service.resourcemanagement.MetadataDataAccessUtils;
import bio.terra.datarepo.service.resourcemanagement.azure.AzureApplicationDeploymentResource;
import bio.terra.datarepo.service.resourcemanagement.google.GoogleProjectResource;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.commons.lang3.StringUtils;

public final class ResourceFixtures {
  private ResourceFixtures() {}

  // Build the a random project resource given the billing profile in support of
  // DAO-only unit tests. This is ready to be used as a request to the GoogleResourceDao
  public static GoogleProjectResource randomProjectResource(BillingProfileModel billingProfile) {
    return new GoogleProjectResource()
        .googleProjectId(ProfileFixtures.randomizeName("fake-test-project-id"))
        .googleProjectNumber(shuffleString("123456789012"))
        .profileId(billingProfile.getId());
  }

  public static AzureApplicationDeploymentResource randomApplicationDeploymentResource(
      BillingProfileModel billingProfile) {
    return new AzureApplicationDeploymentResource()
        .azureApplicationDeploymentId(
            MetadataDataAccessUtils.getApplicationDeploymentId(billingProfile))
        .azureApplicationDeploymentName(billingProfile.getApplicationDeploymentName())
        .azureResourceGroupName(billingProfile.getResourceGroupName())
        .azureSynapseWorkspaceName(ProfileFixtures.randomizeName("synapse"))
        .defaultRegion(AzureRegion.DEFAULT_AZURE_REGION)
        .storageAccountSkuType(AzureStorageAccountSkuType.STANDARD_LRS)
        .storageAccountPrefix("tdr")
        .profileId(billingProfile.getId());
  }

  public static String shuffleString(String input) {
    List<String> newProjectNum = Arrays.asList(input.split(""));
    Collections.shuffle(newProjectNum);
    return StringUtils.join(newProjectNum, "");
  }
}
