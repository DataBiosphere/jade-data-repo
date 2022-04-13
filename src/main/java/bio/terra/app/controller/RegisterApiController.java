package bio.terra.app.controller;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.controller.RegisterApi;
import bio.terra.model.UserStatusInfo;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.auth.iam.PolicyMemberValidator;
import bio.terra.service.dataset.AssetModelValidator;
import com.fasterxml.jackson.databind.ObjectMapper;
import io.swagger.annotations.Api;
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

@Controller
@Api(tags = {"register"})
public class RegisterApiController implements RegisterApi {

  private Logger logger = LoggerFactory.getLogger(RegisterApiController.class);

  private final ObjectMapper objectMapper;
  private final HttpServletRequest request;
  private final IamService iamService;
  private final PolicyMemberValidator policyMemberValidator;
  private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  private final AssetModelValidator assetModelValidator;

  @Autowired
  public RegisterApiController(
      ObjectMapper objectMapper,
      HttpServletRequest request,
      IamService iamService,
      PolicyMemberValidator policyMemberValidator,
      AuthenticatedUserRequestFactory authenticatedUserRequestFactory,
      AssetModelValidator assetModelValidator) {
    this.objectMapper = objectMapper;
    this.request = request;
    this.iamService = iamService;
    this.policyMemberValidator = policyMemberValidator;
    this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
    this.assetModelValidator = assetModelValidator;
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
  public ResponseEntity<UserStatusInfo> user() {
    UserStatusInfo info = iamService.getUserInfo(getAuthenticatedInfo());
    return new ResponseEntity<>(info, HttpStatus.OK);
  }
}
