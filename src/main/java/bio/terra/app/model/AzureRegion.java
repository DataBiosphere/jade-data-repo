package bio.terra.app.model;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

/** Valid regions in Azure. */
public enum AzureRegion implements CloudRegion {
  EAST_US("eastus"),
  EAST_US_2("eastus2"),
  SOUTH_CENTRAL_US("southcentralus"),
  WEST_US_2("westus2"),
  WEST_US_3("westus3"),
  AUSTRALIA_EAST("australiaeast"),
  SOUTHEAST_ASIA("southeastasia"),
  NORTH_EUROPE("northeurope"),
  SWEDEN_CENTRAL("swedencentral"),
  UK_SOUTH("uksouth"),
  WEST_EUROPE("westeurope"),
  CENTRAL_US("centralus"),
  NORTH_CENTRAL_US("northcentralus"),
  WEST_US("westus"),
  SOUTH_AFRICA_NORTH("southafricanorth"),
  CENTRAL_INDIA("centralindia"),
  EAST_ASIA("eastasia"),
  JAPAN_EAST("japaneast"),
  JIO_INDIA_WEST("jioindiawest"),
  KOREA_CENTRAL("koreacentral"),
  CANADA_CENTRAL("canadacentral"),
  FRANCE_CENTRAL("francecentral"),
  GERMANY_WEST_CENTRAL("germanywestcentral"),
  NORWAY_EAST("norwayeast"),
  SWITZERLAND_NORTH("switzerlandnorth"),
  UAE_NORTH("uaenorth"),
  BRAZIL_SOUTH("brazilsouth"),
  CENTRAL_US_STAGE("centralusstage"),
  EAST_US_STAGE("eastusstage"),
  EAST_US_2_STAGE("eastus2stage"),
  NORTH_CENTRAL_US_STAGE("northcentralusstage"),
  SOUTH_CENTRAL_US_STAGE("southcentralusstage"),
  WEST_US_STAGE("westusstage"),
  WEST_US_2_STAGE("westus2stage"),
  ASIA("asia"),
  ASIA_PACIFIC("asiapacific"),
  AUSTRALIA("australia"),
  BRAZIL("brazil"),
  CANADA("canada"),
  EUROPE("europe"),
  GLOBAL("global"),
  INDIA("india"),
  JAPAN("japan"),
  UNITED_KINGDOM("uk"),
  UNITED_STATES("unitedstates"),
  EAST_ASIA_STAGE("eastasiastage"),
  SOUTHEAST_ASIA_STAGE("southeastasiastage"),
  CENTRAL_US_EUAP("centraluseuap"),
  EAST_US_2_EUAP("eastus2euap"),
  WEST_CENTRAL_US("westcentralus"),
  SOUTH_AFRICA_WEST("southafricawest"),
  AUSTRALIA_CENTRAL("australiacentral"),
  AUSTRALIA_CENTRAL_2("australiacentral2"),
  AUSTRALIA_SOUTHEAST("australiasoutheast"),
  JAPAN_WEST("japanwest"),
  JIO_INDIA_CENTRAL("jioindiacentral"),
  KOREA_SOUTH("koreasouth"),
  SOUTH_INDIA("southindia"),
  WEST_INDIA("westindia"),
  CANADA_EAST("canadaeast"),
  FRANCE_SOUTH("francesouth"),
  GERMANY_NORTH("germanynorth"),
  NORWAY_WEST("norwaywest"),
  SWEDEN_SOUTH("swedensouth"),
  SWITZERLAND_WEST("switzerlandwest"),
  UK_WEST("ukwest"),
  UAE_CENTRAL("uaecentral"),
  BRAZIL_SOUTHEAST("brazilsoutheast");

  public static final AzureRegion DEFAULT_AZURE_REGION = AzureRegion.CENTRAL_US;

  public static final List<String> SUPPORTED_REGIONS =
      Arrays.stream(AzureRegion.values())
          .map(AzureRegion::toString)
          .collect(Collectors.toUnmodifiableList());

  private final String value;

  AzureRegion(String value) {
    this.value = value;
  }

  @Override
  public String getValue() {
    return value;
  }

  public String toString() {
    return value;
  }

  /**
   * Differs from {@link #valueOf(String)} in that this does not fail if no value is found, and uses
   * a case insensitive compare
   *
   * @param text region to look up
   * @return An AzureRegion or null if none is found
   */
  public static AzureRegion fromName(String text) {
    for (AzureRegion region : AzureRegion.values()) {
      if (region.name().equalsIgnoreCase(text)) {
        return region;
      }
    }
    return null;
  }

  public static AzureRegion fromValue(String text) {
    for (AzureRegion region : AzureRegion.values()) {
      if (region.value.equalsIgnoreCase(text)) {
        return region;
      }
    }
    return null;
  }

  public static AzureRegion fromValueWithDefault(String text) {
    return Optional.ofNullable(AzureRegion.fromValue(text))
        .orElse(AzureRegion.DEFAULT_AZURE_REGION);
  }
}
