package bio.terra.common;

import static org.junit.jupiter.api.Assertions.*;

import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import java.util.Map;
import java.util.function.Supplier;
import java.util.stream.Stream;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

@Tag(Unit.TAG)
class CloudPlatformWrapperTest {

  public static Stream<Arguments> platformSource() {
    return Stream.of(
        Arguments.of(CloudPlatform.GCP, "GCP"), Arguments.of(CloudPlatform.AZURE, "Azure"));
  }

  static final Map<CloudPlatform, Supplier<String>> CLOUD_MAP =
      Map.of(CloudPlatform.GCP, () -> "GCP", CloudPlatform.AZURE, () -> "Azure");

  @ParameterizedTest
  @MethodSource("platformSource")
  void chooseMap(CloudPlatform cloudPlatform, String expected) {
    assertEquals(expected, CloudPlatformWrapper.of(cloudPlatform).choose(CLOUD_MAP));
  }

  @ParameterizedTest
  @MethodSource("platformSource")
  void chooseFunction(CloudPlatform cloudPlatform, String expected) {
    assertEquals(
        expected, CloudPlatformWrapper.of(cloudPlatform).choose(() -> "GCP", () -> "Azure"));
  }

  @ParameterizedTest
  @MethodSource("platformSource")
  void chooseValue(CloudPlatform cloudPlatform, String expected) {
    assertEquals(expected, CloudPlatformWrapper.of(cloudPlatform).choose("GCP", "Azure"));
  }
}
