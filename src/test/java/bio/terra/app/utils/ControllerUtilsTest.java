package bio.terra.app.utils;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;

import bio.terra.app.controller.exception.ValidationException;
import bio.terra.common.category.Unit;
import bio.terra.model.JobModel;
import bio.terra.model.JobModel.JobStatusEnum;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

@Tag(Unit.TAG)
class ControllerUtilsTest {

  @Test
  void testValidateEnumerateParamsOffset() {
    assertThrows(ValidationException.class, () -> ControllerUtils.validateEnumerateParams(-1, 1));
  }

  @Test
  void testValidateEnumerateParamsLimit() {
    assertThrows(ValidationException.class, () -> ControllerUtils.validateEnumerateParams(0, -1));
  }

  @Test
  void testValidateEnumerateParamsOk() {
    ControllerUtils.validateEnumerateParams(0, 1);
  }

  @Test
  void testJobToResponse() {
    var jobModel = new JobModel();
    jobModel.setId("id");

    jobModel.setJobStatus(JobStatusEnum.RUNNING);
    ResponseEntity<JobModel> entity = ControllerUtils.jobToResponse(jobModel);
    assertThat(entity.getStatusCode(), is(HttpStatus.ACCEPTED));
    assertThat(entity.getHeaders().getFirst("Location"), is("/api/repository/v1/jobs/id"));

    jobModel.setJobStatus(JobStatusEnum.SUCCEEDED);
    entity = ControllerUtils.jobToResponse(jobModel);
    assertThat(entity.getStatusCode(), is(HttpStatus.OK));
    assertThat(entity.getHeaders().getFirst("Location"), is("/api/repository/v1/jobs/id/result"));
  }
}
