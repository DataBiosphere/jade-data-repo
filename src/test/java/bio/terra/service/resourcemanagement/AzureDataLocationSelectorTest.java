package bio.terra.service.resourcemanagement;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.params.provider.Arguments.arguments;

import bio.terra.app.model.AzureRegion;
import bio.terra.model.BillingProfileModel;
import java.util.UUID;
import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Tag("bio.terra.common.category.Unit")
class AzureDataLocationSelectorTest {

  private static final BillingProfileModel PROFILE_1 =
      new BillingProfileModel().id(UUID.randomUUID()).profileName("foo");
  private static final BillingProfileModel PROFILE_2 =
      new BillingProfileModel().id(UUID.randomUUID()).profileName("bar");

  private static final AzureRegion REGION_1 = AzureRegion.ASIA;
  private static final AzureRegion REGION_2 = AzureRegion.EAST_US;

  private final AzureDataLocationSelector dataLocationSelector = new AzureDataLocationSelector();

  @Test
  void testGenerateNamePrefixIsCorrect() {
    assertThat(
        "prefix is correct",
        dataLocationSelector.createStorageAccountName("test", REGION_1, PROFILE_1, false),
        startsWith("test"));
  }

  @Test
  void testGenerateNameValuesAreRepeatable() {
    assertThat(
        "values are repeatable",
        dataLocationSelector.createStorageAccountName("test", REGION_1, PROFILE_1, false),
        is(dataLocationSelector.createStorageAccountName("test", REGION_1, PROFILE_1, false)));
  }

  private record Parameters(
      String prefix,
      AzureRegion region,
      BillingProfileModel profile,
      boolean secureMonitoringEnabled) {}

  private static Stream<Arguments> testGeneratedNamesAreDifferent() {
    return Stream.of(
        arguments(
            "prefix is different",
            new Parameters("foo", REGION_1, PROFILE_1, false),
            new Parameters("bar", REGION_1, PROFILE_1, false)),
        arguments(
            "region is different",
            new Parameters("foo", REGION_1, PROFILE_1, false),
            new Parameters("foo", REGION_2, PROFILE_1, false)),
        arguments(
            "profile is different",
            new Parameters("foo", REGION_1, PROFILE_1, false),
            new Parameters("foo", REGION_1, PROFILE_2, false)),
        arguments(
            "secure monitoring is different",
            new Parameters("foo", REGION_1, PROFILE_1, false),
            new Parameters("foo", REGION_1, PROFILE_1, true)));
  }

  @ParameterizedTest
  @MethodSource
  void testGeneratedNamesAreDifferent(
      String message, Parameters parameters1, Parameters parameters2) {
    assertThat(
        "names don't match when the " + message,
        dataLocationSelector.createStorageAccountName(
            parameters1.prefix,
            parameters1.region,
            parameters1.profile,
            parameters1.secureMonitoringEnabled),
        is(
            not(
                dataLocationSelector.createStorageAccountName(
                    parameters2.prefix,
                    parameters2.region,
                    parameters2.profile,
                    parameters2.secureMonitoringEnabled))));
  }
}
