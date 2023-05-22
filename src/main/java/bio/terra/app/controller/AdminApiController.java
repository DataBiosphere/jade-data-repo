package bio.terra.app.controller;

import bio.terra.app.utils.ControllerUtils;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.controller.AdminApi;
import bio.terra.model.DrsAliasModel;
import bio.terra.model.JobModel;
import bio.terra.service.filedata.DrsService;
import bio.terra.service.job.JobService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.Api;
import java.util.List;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

@Controller
@Api(tags = {"admin"})
public class AdminApiController implements AdminApi {

  private Logger logger = LoggerFactory.getLogger(AdminApiController.class);

  private final ObjectMapper objectMapper;
  private final HttpServletRequest request;
  private final JobService jobService;
  private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  private final DrsService drsService;

  @Autowired
  public AdminApiController(
      ObjectMapper objectMapper,
      HttpServletRequest request,
      JobService jobService,
      AuthenticatedUserRequestFactory authenticatedUserRequestFactory,
      DrsService drsService) {
    this.objectMapper = objectMapper;
    this.request = request;
    this.jobService = jobService;
    this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
    this.drsService = drsService;
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

  @Override
  public ResponseEntity<JobModel> registerDrsAliases(List<DrsAliasModel> aliases) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    String jobId = drsService.registerDrsAliases(aliases, userReq);
    return ControllerUtils.jobToResponse(jobService.retrieveJob(jobId, userReq));
  }
}
