package bio.terra.service.job;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.buffer.client.ApiException;
import bio.terra.common.category.Unit;
import bio.terra.service.resourcemanagement.exception.BufferServiceAPIException;
import bio.terra.stairway.Step;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;

@Tag(Unit.TAG)
class StairwayExceptionSerializerTest {

  private StairwayExceptionSerializer stairwayExceptionSerializer;

  private String serializedException;

  @BeforeEach
  void setUp() {
    ObjectMapper objectMapper = new ApplicationConfiguration().objectMapper();
    stairwayExceptionSerializer = new StairwayExceptionSerializer(objectMapper);

    var originalException =
        new BufferServiceAPIException(
            new ApiException(
                "No projects are available in the datarepo pool",
                HttpStatus.NOT_FOUND.value(),
                null,
                null));
    // Fake providing step info in stack trace
    StackTraceElement[] fakeStackTrace = {
      new StackTraceElement(Step.class.getName(), "method", "file", 0)
    };
    originalException.setStackTrace(fakeStackTrace);

    serializedException = stairwayExceptionSerializer.serialize(originalException);
  }

  @Test
  void serializeTest() {
    assertThat(
        serializedException,
        equalTo(
            """
                {"className":"bio.terra.service.resourcemanagement.exception.BufferServiceAPIException","message":"Error from Buffer Service","errorDetails":[null],"dataRepoException":true}"""));
  }

  @Test
  void deserializeDataRepoException() {
    var deserializedException = stairwayExceptionSerializer.deserialize(serializedException);
    assertThat(deserializedException.getMessage(), equalTo("Error from Buffer Service"));
  }
}
