package bio.terra.common;

import static org.junit.jupiter.api.Assertions.*;

import bio.terra.common.category.Unit;
import bio.terra.model.CloudPlatform;
import java.util.Map;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class CloudPlatformWrapperTest {

  @Test
  void choose_Gcp() {
    assertEquals(
        "GCP",
        CloudPlatformWrapper.of(CloudPlatform.GCP)
            .choose(Map.of(CloudPlatform.GCP, () -> "GCP", CloudPlatform.AZURE, () -> "Azure")));
  }

  @Test
  void choose_Azure() {
    assertEquals(
        "Azure",
        CloudPlatformWrapper.of(CloudPlatform.AZURE)
            .choose(Map.of(CloudPlatform.GCP, () -> "GCP", CloudPlatform.AZURE, () -> "Azure")));
  }
}
