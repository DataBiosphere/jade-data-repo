package bio.terra.service.job;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.common.category.Unit;
import bio.terra.service.resourcemanagement.exception.BufferServiceAPIException;
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
    ObjectMapper objectMapper = new ApplicationConfiguration().objectMapper();
    stairwayExceptionSerializer = new StairwayExceptionSerializer(objectMapper);
  }

  @Test
  public void deserializeDataRepoException() {
    var serializedException =
        """
        {"className":"bio.terra.service.resourcemanagement.exception.BufferServiceAPIException","message":"Error from Buffer Service",
        "errorDetails":["The step failed for an unknown reason.","Please contact the TDR team for help."],"dataRepoException":true}
        """;
    var deserializedException = stairwayExceptionSerializer.deserialize(serializedException);
    assertThat(deserializedException.getMessage(), equalTo("Error from Buffer Service"));
  }

  @Test
  public void deserializeErrorCode() {
    var serializedException =
        """
        {"className":"bio.terra.service.resourcemanagement.exception.BufferServiceAPIException","message":"Error from Buffer Service",
        "errorDetails":["The step failed for an unknown reason.","Please contact the TDR team for help."],"dataRepoException":true,"apiErrorReportException":true,"errorCode":"429"}
        """;
    BufferServiceAPIException deserializedException =
        (BufferServiceAPIException) stairwayExceptionSerializer.deserialize(serializedException);
    assertThat(deserializedException.getMessage(), equalTo("Error from Buffer Service"));
    assertThat(deserializedException.getApiExceptionStatus(), equalTo(429));
  }
}
