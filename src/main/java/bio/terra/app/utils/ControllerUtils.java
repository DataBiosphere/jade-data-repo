package bio.terra.app.utils;

import bio.terra.app.controller.exception.ValidationException;
import bio.terra.model.JobModel;
import org.apache.commons.lang3.StringUtils;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public final class ControllerUtils {

    // constant used for validation
    private static final List<String> VALID_SORT_OPTIONS = Arrays.asList("name", "description", "created_date");
    private static final List<String> VALID_DIRECTION_OPTIONS = Arrays.asList("asc", "desc");

    private ControllerUtils() {
    }

    public static void validateEnumerateParams(Integer offset, Integer limit, String sort, String direction) {
        List<String> errors = new ArrayList<>();
        if (offset < 0) {
            errors.add("offset must be greater than or equal to 0.");
        }
        if (limit < 1) {
            errors.add("limit must be greater than or equal to 1.");
        }
        if (!StringUtils.isEmpty(sort) && !VALID_SORT_OPTIONS.contains(sort)) {
            errors.add(String.format("sort must be one of: (%s).", String.join(", ", VALID_SORT_OPTIONS)));
        }
        if (!StringUtils.isEmpty(direction) && !VALID_DIRECTION_OPTIONS.contains(direction)) {
            errors.add("direction must be one of: (asc, desc).");
        }
        if (!errors.isEmpty()) {
            throw new ValidationException("Invalid enumerate parameter(s).", errors);
        }
    }

    public static void validateEnumerateParams(Integer offset, Integer limit) {
        validateEnumerateParams(offset, limit, null, null);
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
