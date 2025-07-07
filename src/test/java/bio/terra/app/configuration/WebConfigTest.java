package bio.terra.app.configuration;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;

import bio.terra.common.category.Unit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class WebConfigTest {

  @Test
  void swaggerUiVersionMatchesClasspath() {
    assertDoesNotThrow(WebConfig::getSwaggerUiVersion);
  }
}
