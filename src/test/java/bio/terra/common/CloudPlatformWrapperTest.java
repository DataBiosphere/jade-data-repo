package bio.terra.common;

import static org.junit.jupiter.api.Assertions.*;

import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import java.util.Map;
import java.util.function.Supplier;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;

@Tag(Unit.TAG)
class CloudPlatformWrapperTest {

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void choose_Map(CloudPlatform cloudPlatform) {
    CloudPlatformWrapper cloudWrapper = (CloudPlatformWrapper.of(cloudPlatform));
    Map<CloudPlatform, Supplier<String>> cloudMap =
        Map.of(CloudPlatform.GCP, () -> "GCP", CloudPlatform.AZURE, () -> "Azure");
    if (cloudWrapper.isGcp()) {
      assertEquals("GCP", cloudWrapper.choose(cloudMap));
    }
    if (cloudWrapper.isAzure()) {
      assertEquals("Azure", cloudWrapper.choose(cloudMap));
    }
  }

  @ParameterizedTest
  @EnumSource(CloudPlatform.class)
  void choose(CloudPlatform cloudPlatform) {
    CloudPlatformWrapper cloudWrapper = (CloudPlatformWrapper.of(cloudPlatform));
    if (cloudWrapper.isGcp()) {
      assertEquals("GCP", cloudWrapper.choose(() -> "GCP", () -> "Azure"));
    }
    if (cloudWrapper.isAzure()) {
      assertEquals("Azure", cloudWrapper.choose(() -> "GCP", () -> "Azure"));
    }
  }
}
