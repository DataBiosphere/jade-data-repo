package bio.terra.datarepo.app.utils;

import static org.junit.Assert.assertEquals;

import bio.terra.datarepo.app.controller.exception.ValidationException;
import bio.terra.datarepo.common.category.Unit;
import bio.terra.datarepo.model.JobModel;
import bio.terra.datarepo.model.JobModel.JobStatusEnum;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

@Category(Unit.class)
public class ControllerUtilsTest {

  @Test(expected = ValidationException.class)
  public void testValidateEnumerateParamsOffset() {
    ControllerUtils.validateEnumerateParams(-1, 1);
  }

  @Test(expected = ValidationException.class)
  public void testValidateEnumerateParamsLimit() {
    ControllerUtils.validateEnumerateParams(0, -1);
  }

  @Test
  public void testValidateEnumerateParamsOk() {
    ControllerUtils.validateEnumerateParams(0, 1);
  }

  @Test
  public void testJobToResponse() {
    var jobModel = new JobModel();
    jobModel.setId("id");

    jobModel.setJobStatus(JobStatusEnum.RUNNING);
    ResponseEntity<JobModel> entity = ControllerUtils.jobToResponse(jobModel);
    assertEquals(HttpStatus.ACCEPTED, entity.getStatusCode());
    assertEquals("/api/repository/v1/jobs/id", entity.getHeaders().getFirst("Location"));

    jobModel.setJobStatus(JobStatusEnum.SUCCEEDED);
    entity = ControllerUtils.jobToResponse(jobModel);
    assertEquals(HttpStatus.OK, entity.getStatusCode());
    assertEquals("/api/repository/v1/jobs/id/result", entity.getHeaders().getFirst("Location"));
  }
}
