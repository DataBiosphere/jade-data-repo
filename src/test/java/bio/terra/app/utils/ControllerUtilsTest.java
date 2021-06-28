package bio.terra.app.utils;

import bio.terra.app.controller.exception.ValidationException;
import bio.terra.common.category.Unit;
import bio.terra.model.JobModel.JobStatusEnum;
import bio.terra.model.JobModel;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

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
        ResponseEntity entity = ControllerUtils.jobToResponse(jobModel);
        assertEquals(entity.getStatusCode(), HttpStatus.ACCEPTED);
        assertTrue(entity.getHeaders().containsKey("Location"));
        assertEquals(entity.getHeaders().getFirst("Location"), "/api/repository/v1/jobs/id");

        jobModel.setJobStatus(JobStatusEnum.SUCCEEDED);
        entity = ControllerUtils.jobToResponse(jobModel);
        assertEquals(entity.getStatusCode(), HttpStatus.OK);
        assertTrue(entity.getHeaders().containsKey("Location"));
        assertEquals(entity.getHeaders().getFirst("Location"), "/api/repository/v1/jobs/id/result");
    }

}
