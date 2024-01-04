package bio.terra.app.controller;

import bio.terra.app.utils.ControllerUtils;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.controller.UpgradeApi;
import bio.terra.model.JobModel;
import bio.terra.model.UpgradeModel;
import bio.terra.service.auth.iam.PolicyMemberValidator;
import bio.terra.service.dataset.AssetModelValidator;
import bio.terra.service.job.JobService;
import bio.terra.service.upgrade.UpgradeService;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.Api;
import java.util.Optional;
import javax.servlet.http.HttpServletRequest;
import javax.validation.Valid;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.WebDataBinder;
import org.springframework.web.bind.annotation.InitBinder;
import org.springframework.web.bind.annotation.RequestBody;

@Controller
@Api(tags = {"upgrade"})
public class UpgradeApiController implements UpgradeApi {

  private Logger logger = LoggerFactory.getLogger(UpgradeApiController.class);

  private final ObjectMapper objectMapper;
  private final HttpServletRequest request;
  private final JobService jobService;
  private final PolicyMemberValidator policyMemberValidator;
  private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  private final AssetModelValidator assetModelValidator;
  private final UpgradeService upgradeService;

  @Autowired
  public UpgradeApiController(
      ObjectMapper objectMapper,
      HttpServletRequest request,
      JobService jobService,
      PolicyMemberValidator policyMemberValidator,
      AuthenticatedUserRequestFactory authenticatedUserRequestFactory,
      AssetModelValidator assetModelValidator,
      UpgradeService upgradeService) {
    this.objectMapper = objectMapper;
    this.request = request;
    this.jobService = jobService;
    this.policyMemberValidator = policyMemberValidator;
    this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
    this.assetModelValidator = assetModelValidator;
    this.upgradeService = upgradeService;
  }

  @InitBinder
  protected void initBinder(final WebDataBinder binder) {
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

  @Override
  public ResponseEntity<JobModel> upgrade(@Valid @RequestBody UpgradeModel request) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    String jobId = upgradeService.upgrade(request, userReq);
    return ControllerUtils.jobToResponse(jobService.retrieveJob(jobId, userReq));
  }
}
