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
@SpringBootTest(properties = {"datarepo.testWithDatabase=false"})
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class CloudRegionTest {

  @Autowired private ObjectMapper objectMapper;

  @Test
  public void testDeserializeHappyPath() throws JsonProcessingException {
    assertThat(
        objectMapper.readValue("\"EUROPE_WEST1\"", CloudRegion.class),
        equalTo(GoogleRegion.EUROPE_WEST1));
    assertThat(
        objectMapper.readValue("\"FRANCE_SOUTH\"", CloudRegion.class),
        equalTo(AzureRegion.FRANCE_SOUTH));
  }

  @Test
  public void testDeserializeBadRegionValue() {
    assertThat(
        assertThrows(
                InvalidFormatException.class,
                () -> objectMapper.readValue("\"EUROPE_WEST100\"", CloudRegion.class),
                "bad regions don't deserialize")
            .getMessage(),
        containsString("Unrecognized CloudRegion: EUROPE_WEST100"));
  }

  @Test
  public void testDeserializeBadRegionFormat() {
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
