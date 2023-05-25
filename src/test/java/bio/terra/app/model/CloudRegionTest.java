package bio.terra.app.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.app.configuration.ApplicationConfiguration;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.ContextConfiguration;

@ActiveProfiles({"google", "unittest"})
@ContextConfiguration(classes = ApplicationConfiguration.class)
@Tag("bio.terra.common.category.Unit")
@WebMvcTest(properties = {"datarepo.testWithEmbeddedDatabase=false"})
public class CloudRegionTest {

  @Autowired private ObjectMapper objectMapper;

  @Test
  void testDeserializeHappyPath() throws JsonProcessingException {
    assertThat(
        objectMapper.readValue("\"EUROPE_WEST1\"", CloudRegion.class),
        equalTo(GoogleRegion.EUROPE_WEST1));
    assertThat(
        objectMapper.readValue("\"FRANCE_SOUTH\"", CloudRegion.class),
        equalTo(AzureRegion.FRANCE_SOUTH));
  }

  @Test
  void testDeserializeBadRegionValue() {
    assertThat(
        assertThrows(
                InvalidFormatException.class,
                () -> objectMapper.readValue("\"EUROPE_WEST100\"", CloudRegion.class),
                "bad regions don't deserialize")
            .getMessage(),
        containsString("Unrecognized CloudRegion: EUROPE_WEST100"));
  }

  @Test
  void testDeserializeBadRegionFormat() {
    assertThat(
        assertThrows(
                InvalidFormatException.class,
                () -> objectMapper.readValue("123", CloudRegion.class),
                "numbers don't deserialize")
            .getMessage(),
        containsString("Invalid representation of CloudRegion"));

    assertThat(
        assertThrows(
                InvalidFormatException.class,
                () -> objectMapper.readValue("{\"foo\": \"bar\"}", CloudRegion.class),
                "objects don't deserialize")
            .getMessage(),
        containsString("Invalid representation of CloudRegion"));
  }
}
