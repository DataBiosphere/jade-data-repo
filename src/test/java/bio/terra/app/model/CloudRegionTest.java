package bio.terra.app.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag("bio.terra.common.category.Unit")
class CloudRegionTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  void testDeserializeHappyPath() throws JsonProcessingException {
    assertThat(
        OBJECT_MAPPER.readValue("\"EUROPE_WEST1\"", CloudRegion.class),
        equalTo(GoogleRegion.EUROPE_WEST1));
    assertThat(
        OBJECT_MAPPER.readValue("\"FRANCE_SOUTH\"", CloudRegion.class),
        equalTo(AzureRegion.FRANCE_SOUTH));
  }

  @Test
  void testDeserializeBadRegionValue() {
    assertThat(
        assertThrows(
                InvalidFormatException.class,
                () -> OBJECT_MAPPER.readValue("\"EUROPE_WEST100\"", CloudRegion.class),
                "bad regions don't deserialize")
            .getMessage(),
        containsString("Unrecognized CloudRegion: EUROPE_WEST100"));
  }

  @Test
  void testDeserializeBadRegionFormat() {
    assertThat(
        assertThrows(
                InvalidFormatException.class,
                () -> OBJECT_MAPPER.readValue("123", CloudRegion.class),
                "numbers don't deserialize")
            .getMessage(),
        containsString("Invalid representation of CloudRegion"));

    assertThat(
        assertThrows(
                InvalidFormatException.class,
                () -> OBJECT_MAPPER.readValue("{\"foo\": \"bar\"}", CloudRegion.class),
                "objects don't deserialize")
            .getMessage(),
        containsString("Invalid representation of CloudRegion"));
  }
}
