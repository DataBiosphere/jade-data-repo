package bio.terra.service.job;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.*;

import bio.terra.common.category.Unit;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@Tag(Unit.TAG)
public class StairwayExceptionSerializerTest {

  private StairwayExceptionSerializer stairwayExceptionSerializer;

  @BeforeEach
  void setUp() {
    ObjectMapper objectMapper = new ObjectMapper();
    stairwayExceptionSerializer = new StairwayExceptionSerializer(objectMapper);
  }

  @Test
  public void deserialize() {
    var serializedException =
        "{\"className\":\"bio.terra.service.resourcemanagement.exception.BufferServiceAPIException\",\"message\":\"Error from Buffer Service\",\"errorDetails\":[\"The step failed for an unknown reason.\",\"Please contact the TDR team for help.\"],\"dataRepoException\":true}";
    var deserializedException = stairwayExceptionSerializer.deserialize(serializedException);
    assertThat(deserializedException.getMessage(), equalTo("Error from Buffer Service"));
  }
}
