package bio.terra.app.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.category.Unit;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;

@Tag(Unit.TAG)
class CloudResourceTest {

  private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();

  @Test
  void testDeserializeHappyPath() throws JsonProcessingException {
    assertThat(
        OBJECT_MAPPER.readValue("\"BIGQUERY\"", CloudResource.class),
        equalTo(GoogleCloudResource.BIGQUERY));
    assertThat(
        OBJECT_MAPPER.readValue("\"STORAGE_ACCOUNT\"", CloudResource.class),
        equalTo(AzureCloudResource.STORAGE_ACCOUNT));
  }

  @Test
  void testDeserializeBadResourceValue() {
    assertThat(
        assertThrows(
                InvalidFormatException.class,
                () -> OBJECT_MAPPER.readValue("\"SMALLQUERY\"", CloudResource.class),
                "bad resources don't deserialize")
            .getMessage(),
        containsString("Unrecognized CloudResource: SMALLQUERY"));
  }

  @Test
  void testDeserializeBadResourceFormat() {
    assertThat(
        assertThrows(
                InvalidFormatException.class,
                () -> OBJECT_MAPPER.readValue("123", CloudResource.class),
                "numbers don't deserialize")
            .getMessage(),
        containsString("Invalid representation of CloudResource"));

    assertThat(
        assertThrows(
                InvalidFormatException.class,
                () -> OBJECT_MAPPER.readValue("{\"foo\": \"bar\"}", CloudResource.class),
                "objects don't deserialize")
            .getMessage(),
        containsString("Invalid representation of CloudResource"));
  }
}
