package bio.terra.common.fixtures;

import bio.terra.model.BillingProfileModel;
import bio.terra.model.BillingProfileRequestModel;
import bio.terra.model.CloudPlatform;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;
import java.util.UUID;

public final class ProfileFixtures {
  private ProfileFixtures() {}

  private static Random randomGenerator = new Random();

  public static String randomHex(int n) {
    StringBuffer sb = new StringBuffer();
    while (sb.length() < n) {
      sb.append(Integer.toHexString(randomGenerator.nextInt(16)));
    }
    return sb.toString();
  }

  // Google billing account ids are three sets of six capitalized hex characters separated by dashes
  public static String randomBillingAccountId() {
    List<String> groups = new ArrayList<>();
    for (int i = 0; i < 3; i++) {
      groups.add(randomHex(6));
    }
    return String.join("-", groups).toUpperCase();
  }

  public static BillingProfileModel billingProfileForAccount(final String accountId) {
    return new BillingProfileModel()
        .id(UUID.randomUUID())
        .cloudPlatform(CloudPlatform.GCP)
        .billingAccountId(accountId)
        .profileName(randomizeName("test-profile"))
        .biller("direct")
        .description("test profile description");
  }

  public static BillingProfileModel billingProfileForDeployedApplication(
      final UUID tenantId,
      final UUID subscriptionId,
      final String resourceGroupName,
      final String applicationDeploymentName) {
    return new BillingProfileModel()
        .id(UUID.randomUUID())
        .cloudPlatform(CloudPlatform.AZURE)
        .tenantId(tenantId)
        .subscriptionId(subscriptionId)
        .resourceGroupName(resourceGroupName)
        .applicationDeploymentName(applicationDeploymentName)
        .profileName(randomizeName("test-profile"))
        .biller("direct")
        .description("test profile description (Azure");
  }

  public static BillingProfileModel randomBillingProfile() {
    return billingProfileForAccount(randomBillingAccountId());
  }

  public static BillingProfileModel randomAzureBillingProfile() {
    return billingProfileForDeployedApplication(
        UUID.randomUUID(),
        UUID.randomUUID(),
        randomizeName("resourcegroup"),
        randomizeName("appdeployment"));
  }

  public static BillingProfileRequestModel billingProfileRequest(
      final BillingProfileModel profile) {
    return new BillingProfileRequestModel()
        .id(profile.getId())
        .biller(profile.getBiller())
        .profileName(profile.getProfileName())
        .cloudPlatform(profile.getCloudPlatform())
        .billingAccountId(profile.getBillingAccountId())
        .tenantId(profile.getTenantId())
        .subscriptionId(profile.getSubscriptionId())
        .resourceGroupName(profile.getResourceGroupName())
        .applicationDeploymentName(profile.getApplicationDeploymentName())
        .description(profile.getDescription());
  }

  public static BillingProfileRequestModel randomBillingProfileRequest() {
    return billingProfileRequest(randomBillingProfile());
  }

  public static BillingProfileRequestModel randomizeAzureBillingProfileRequest() {
    return billingProfileRequest(randomAzureBillingProfile());
  }

  public static String randomizeName(String baseName) {
    long suffix = randomGenerator.nextLong();
    return baseName + Long.toUnsignedString(suffix);
  }
}
