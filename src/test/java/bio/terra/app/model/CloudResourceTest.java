package bio.terra.app.model;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;

import bio.terra.common.category.Unit;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest(properties = {"datarepo.testWithEmbeddedDatabase=false"})
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class CloudResourceTest {

  @Autowired private ObjectMapper objectMapper;

  @Test
  public void testDeserializeHappyPath() throws JsonProcessingException {
    assertThat(
        objectMapper.readValue("\"BIGQUERY\"", CloudResource.class),
        equalTo(GoogleCloudResource.BIGQUERY));
    assertThat(
        objectMapper.readValue("\"STORAGE_ACCOUNT\"", CloudResource.class),
        equalTo(AzureCloudResource.STORAGE_ACCOUNT));
  }

  @Test
  public void testDeserializeBadResourceValue() {
    assertThat(
        assertThrows(
                InvalidFormatException.class,
                () -> objectMapper.readValue("\"SMALLQUERY\"", CloudResource.class),
                "bad resources don't deserialize")
            .getMessage(),
        containsString("Unrecognized CloudResource: SMALLQUERY"));
  }

  @Test
  public void testDeserializeBadResourceFormat() {
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
