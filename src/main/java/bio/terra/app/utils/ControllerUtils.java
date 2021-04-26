package bio.terra.app.utils;

import bio.terra.app.controller.exception.ValidationException;
import bio.terra.model.JobModel;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.ArrayList;
import java.util.List;

public final class ControllerUtils {

    private ControllerUtils() {
    }

    public static void validateEnumerateParams(Integer offset, Integer limit) {
        List<String> errors = new ArrayList<>();
        if (offset < 0) {
            errors.add("offset must be greater than or equal to 0.");
        }
        if (limit < 1) {
            errors.add("limit must be greater than or equal to 1.");
        }
        if (!errors.isEmpty()) {
            throw new ValidationException("Invalid enumerate parameter(s).", errors);
        }
    }

    public static ResponseEntity<JobModel> jobToResponse(JobModel job) {
        if (job.getJobStatus() == JobModel.JobStatusEnum.RUNNING) {
            return ResponseEntity
                .status(HttpStatus.ACCEPTED)
                .header("Location", String.format("/api/repository/v1/jobs/%s", job.getId()))
                .body(job);
        } else {
            return ResponseEntity
                .status(HttpStatus.OK)
                .header("Location", String.format("/api/repository/v1/jobs/%s/result", job.getId()))
                .body(job);
        }
    }

}
