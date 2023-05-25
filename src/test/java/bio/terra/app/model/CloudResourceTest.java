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
public class CloudResourceTest {

  @Autowired private ObjectMapper objectMapper;

  @Test
  void testDeserializeHappyPath() throws JsonProcessingException {
    assertThat(
        objectMapper.readValue("\"BIGQUERY\"", CloudResource.class),
        equalTo(GoogleCloudResource.BIGQUERY));
    assertThat(
        objectMapper.readValue("\"STORAGE_ACCOUNT\"", CloudResource.class),
        equalTo(AzureCloudResource.STORAGE_ACCOUNT));
  }

  @Test
  void testDeserializeBadResourceValue() {
    assertThat(
        assertThrows(
                InvalidFormatException.class,
                () -> objectMapper.readValue("\"SMALLQUERY\"", CloudResource.class),
                "bad resources don't deserialize")
            .getMessage(),
        containsString("Unrecognized CloudResource: SMALLQUERY"));
  }

  @Test
  void testDeserializeBadResourceFormat() {
    assertThat(
        assertThrows(
                InvalidFormatException.class,
                () -> objectMapper.readValue("123", CloudResource.class),
                "numbers don't deserialize")
            .getMessage(),
        containsString("Invalid representation of CloudResource"));

    assertThat(
        assertThrows(
                InvalidFormatException.class,
                () -> objectMapper.readValue("{\"foo\": \"bar\"}", CloudResource.class),
                "objects don't deserialize")
            .getMessage(),
        containsString("Invalid representation of CloudResource"));
  }
}
