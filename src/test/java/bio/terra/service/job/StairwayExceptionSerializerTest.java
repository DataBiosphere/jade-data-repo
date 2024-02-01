package bio.terra.service.job;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.buffer.client.ApiException;
import bio.terra.common.category.Unit;
import bio.terra.service.resourcemanagement.exception.BufferServiceAPIException;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@Tag(Unit.TAG)
class StairwayExceptionSerializerTest {

  private StairwayExceptionSerializer stairwayExceptionSerializer;

  private String serializedException;
  private Exception originalException;

  @BeforeEach
  void setUp() {
    ObjectMapper objectMapper = new ApplicationConfiguration().objectMapper();
    stairwayExceptionSerializer = new StairwayExceptionSerializer(objectMapper);

    originalException =
        new BufferServiceAPIException(
            new ApiException(
                "No projects are available in the datarepo pool",
                HttpStatus.NOT_FOUND.value(),
                null,
                null));
    // Fake providing step info in stack trace
    var fakeStackTrace =
        new StackTraceElement(
            "bio.terra.common.GetResourceBufferProjectStep",
            "handoutResource",
            "BufferService.java",
            83);
    originalException.setStackTrace(new StackTraceElement[] {fakeStackTrace});

    serializedException = stairwayExceptionSerializer.serialize(originalException);
  }

  @Test
  void serializeTest() {
    assertThat(
        serializedException,
        equalTo(
            """
                {"apiErrorReportException":true,"className":"bio.terra.service.resourcemanagement.exception.BufferServiceAPIException","message":"Error from Buffer Service","errorDetails":["No projects are available in the datarepo pool"],"errorCode":404,"dataRepoException":true}"""));
  }

  @Test
  void deserializeDataRepoException() {
    var deserializedException = stairwayExceptionSerializer.deserialize(serializedException);
    assertThat(deserializedException.getMessage(), equalTo("Error from Buffer Service"));
    assertThat(deserializedException instanceof BufferServiceAPIException, equalTo(true));
    assertThat(
        "Error code is correctly returned",
        ((BufferServiceAPIException) deserializedException).getApiExceptionStatus(),
        equalTo(404));
    assertThat(
        "Error details are correctly returned",
        ((BufferServiceAPIException) deserializedException).getCauses().get(0),
        equalTo(originalException.getCause().getMessage()));
  }
}
