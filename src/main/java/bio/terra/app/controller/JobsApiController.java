package bio.terra.app.controller;

import static bio.terra.app.utils.ControllerUtils.jobToResponse;

import bio.terra.app.controller.exception.ValidationException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.controller.JobsApi;
import bio.terra.model.JobModel;
import bio.terra.model.SqlSortDirection;
import bio.terra.service.auth.iam.PolicyMemberValidator;
import bio.terra.service.dataset.AssetModelValidator;
import bio.terra.service.dataset.DatasetRequestValidator;
import bio.terra.service.dataset.IngestRequestValidator;
import bio.terra.service.job.JobService;
import bio.terra.service.snapshot.SnapshotRequestValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.Api;
import java.util.List;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@Api(tags = {"jobs"})
public class JobsApiController implements JobsApi {

  private Logger logger = LoggerFactory.getLogger(JobsApiController.class);

  private final ObjectMapper objectMapper;
  private final HttpServletRequest request;
  private final JobService jobService;
  private final DatasetRequestValidator datasetRequestValidator;
  private final SnapshotRequestValidator snapshotRequestValidator;
  private final IngestRequestValidator ingestRequestValidator;
  private final PolicyMemberValidator policyMemberValidator;
  private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  private final AssetModelValidator assetModelValidator;

  @Autowired
  public JobsApiController(
      ObjectMapper objectMapper,
      HttpServletRequest request,
      JobService jobService,
      DatasetRequestValidator datasetRequestValidator,
      SnapshotRequestValidator snapshotRequestValidator,
      IngestRequestValidator ingestRequestValidator,
      PolicyMemberValidator policyMemberValidator,
      AuthenticatedUserRequestFactory authenticatedUserRequestFactory,
      AssetModelValidator assetModelValidator) {
    this.objectMapper = objectMapper;
    this.request = request;
    this.jobService = jobService;
    this.datasetRequestValidator = datasetRequestValidator;
    this.snapshotRequestValidator = snapshotRequestValidator;
    this.ingestRequestValidator = ingestRequestValidator;
    this.policyMemberValidator = policyMemberValidator;
    this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
    this.assetModelValidator = assetModelValidator;
  }

  @InitBinder
  protected void initBinder(final WebDataBinder binder) {
    binder.addValidators(datasetRequestValidator);
    binder.addValidators(snapshotRequestValidator);
    binder.addValidators(ingestRequestValidator);
    binder.addValidators(policyMemberValidator);
    binder.addValidators(assetModelValidator);
  }

  @Override
  public Optional<ObjectMapper> getObjectMapper() {
    return Optional.ofNullable(objectMapper);
  }

  @Override
  public Optional<HttpServletRequest> getRequest() {
    return Optional.ofNullable(request);
  }

  private AuthenticatedUserRequest getAuthenticatedInfo() {
    return authenticatedUserRequestFactory.from(request);
  }

  // -- jobs --
  @Override
  public ResponseEntity<List<JobModel>> enumerateJobs(
      @RequestParam(value = "offset", required = false, defaultValue = "0") Integer offset,
      @RequestParam(value = "limit", required = false, defaultValue = "10") Integer limit,
      @RequestParam(value = "direction", required = false, defaultValue = "desc")
          SqlSortDirection direction,
      @RequestParam(value = "flightClass", required = false, defaultValue = "")
          String flightClass) {
    validiateOffsetAndLimit(offset, limit);
    List<JobModel> results =
        jobService.enumerateJobs(offset, limit, getAuthenticatedInfo(), direction, flightClass);
    return new ResponseEntity<>(results, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<JobModel> retrieveJob(@PathVariable("id") String id) {
    JobModel job = jobService.retrieveJob(id, getAuthenticatedInfo());
    return jobToResponse(job);
  }

  @Override
  public ResponseEntity<Object> retrieveJobResult(@PathVariable("id") String id) {
    JobService.JobResultWithStatus<Object> resultHolder =
        jobService.retrieveJobResult(id, Object.class, getAuthenticatedInfo());
    return ResponseEntity.status(resultHolder.getStatusCode()).body(resultHolder.getResult());
  }

  private void validiateOffsetAndLimit(Integer offset, Integer limit) {
    String errors = "";
    offset = (offset == null) ? offset = 0 : offset;
    if (offset < 0) {
      errors = "Offset must be greater than or equal to 0.";
    }

    limit = (limit == null) ? limit = 10 : limit;
    if (limit < 1) {
      errors += " Limit must be greater than or equal to 1.";
    }
    if (!errors.isEmpty()) {
      throw new ValidationException(errors);
    }
  }
}
