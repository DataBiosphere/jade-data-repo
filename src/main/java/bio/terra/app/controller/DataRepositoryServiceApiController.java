package bio.terra.app.controller;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.controller.exception.TooManyRequestsException;
import bio.terra.common.exception.BadRequestException;
import bio.terra.common.exception.NotFoundException;
import bio.terra.common.exception.NotImplementedException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.controller.DataRepositoryServiceApi;
import bio.terra.model.DRSAccessURL;
import bio.terra.model.DRSAuthorizations;
import bio.terra.model.DRSError;
import bio.terra.model.DRSObject;
import bio.terra.model.DRSPassportRequestModel;
import bio.terra.model.DRSServiceInfo;
import bio.terra.service.auth.iam.exception.IamForbiddenException;
import bio.terra.service.auth.iam.exception.IamUnauthorizedException;
import bio.terra.service.filedata.DrsService;
import bio.terra.service.filedata.exception.InvalidDrsIdException;
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
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

@Controller
@Api(tags = {"DataRepositoryService"})
public class DataRepositoryServiceApiController implements DataRepositoryServiceApi {

  private Logger logger = LoggerFactory.getLogger(DataRepositoryServiceApiController.class);

  private final ObjectMapper objectMapper;
  private final HttpServletRequest request;
  private final DrsService drsService;
  private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;

  // needed for local testing w/o proxy
  private final ApplicationConfiguration appConfig;

  @Autowired
  public DataRepositoryServiceApiController(
      ObjectMapper objectMapper,
      HttpServletRequest request,
      DrsService drsService,
      ApplicationConfiguration appConfig,
      AuthenticatedUserRequestFactory authenticatedUserRequestFactory) {
    this.objectMapper = objectMapper;
    this.request = request;
    this.appConfig = appConfig;
    this.drsService = drsService;
    this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
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

  @ExceptionHandler
  public ResponseEntity<DRSError> badRequestExceptionHandler(BadRequestException ex) {
    DRSError error = new DRSError().msg(ex.getMessage()).statusCode(HttpStatus.BAD_REQUEST.value());
    return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
  }

  @ExceptionHandler
  public ResponseEntity<DRSError> notFoundExceptionHandler(NotFoundException ex) {
    DRSError error = new DRSError().msg(ex.getMessage()).statusCode(HttpStatus.NOT_FOUND.value());
    return new ResponseEntity<>(error, HttpStatus.NOT_FOUND);
  }

  @ExceptionHandler
  public ResponseEntity<DRSError> notImplementedExceptionHandler(NotImplementedException ex) {
    DRSError error =
        new DRSError().msg(ex.getMessage()).statusCode(HttpStatus.NOT_IMPLEMENTED.value());
    return new ResponseEntity<>(error, HttpStatus.NOT_IMPLEMENTED);
  }

  @ExceptionHandler // -- cautionary errors to limit overload
  public ResponseEntity<DRSError> tooManyRequestsExceptionHandler(TooManyRequestsException ex) {
    DRSError error =
        new DRSError().msg(ex.getMessage()).statusCode(HttpStatus.TOO_MANY_REQUESTS.value());
    return new ResponseEntity<>(error, HttpStatus.TOO_MANY_REQUESTS);
  }

  @ExceptionHandler
  public ResponseEntity<DRSError> iAmUnauthorizedExceptionHandler(IamUnauthorizedException ex) {
    DRSError error =
        new DRSError().msg(ex.getMessage()).statusCode(HttpStatus.UNAUTHORIZED.value());
    return new ResponseEntity<>(error, HttpStatus.UNAUTHORIZED);
  }

  @ExceptionHandler
  public ResponseEntity<DRSError> iAmForbiddenExceptionHandler(IamForbiddenException ex) {
    DRSError error = new DRSError().msg(ex.getMessage()).statusCode(HttpStatus.FORBIDDEN.value());
    return new ResponseEntity<>(error, HttpStatus.FORBIDDEN);
  }

  @ExceptionHandler
  public ResponseEntity<DRSError> invalidDrsIdException(InvalidDrsIdException ex) {
    DRSError error = new DRSError().msg(ex.getMessage()).statusCode(HttpStatus.BAD_REQUEST.value());
    return new ResponseEntity<>(error, HttpStatus.BAD_REQUEST);
  }

  @ExceptionHandler
  public ResponseEntity<DRSError> exceptionHandler(Exception ex) {
    DRSError error =
        new DRSError().msg(ex.getMessage()).statusCode(HttpStatus.INTERNAL_SERVER_ERROR.value());
    logger.error("Uncaught exception", ex);
    return new ResponseEntity<>(error, HttpStatus.INTERNAL_SERVER_ERROR);
  }

  @Override
  public ResponseEntity<DRSAccessURL> getAccessURL(
      @PathVariable("object_id") String objectId, @PathVariable("access_id") String accessId) {
    AuthenticatedUserRequest authUser = getAuthenticatedInfo();
    DRSAccessURL accessURL = drsService.getAccessUrlForObjectId(authUser, objectId, accessId);
    return new ResponseEntity<>(accessURL, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<DRSAccessURL> postAccessURL(
      @PathVariable("object_id") String objectId,
      @PathVariable("access_id") String accessId,
      @RequestBody DRSPassportRequestModel drsPassportRequestModel) {
    DRSAccessURL accessURL =
        drsService.postAccessUrlForObjectId(objectId, accessId, drsPassportRequestModel);
    return new ResponseEntity<>(accessURL, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<DRSObject> getObject(
      @PathVariable("object_id") String objectId,
      @RequestParam(value = "expand", required = false, defaultValue = "false") Boolean expand) {
    // The incoming object id is a DRS object id, not a file id.
    AuthenticatedUserRequest authUser = getAuthenticatedInfo();
    DRSObject drsObject = drsService.lookupObjectByDrsId(authUser, objectId, expand);
    return new ResponseEntity<>(drsObject, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<DRSAuthorizations> optionsObject(
      @PathVariable("object_id") String objectId) {
    DRSAuthorizations auths = drsService.lookupAuthorizationsByDrsId(objectId);
    return new ResponseEntity<>(auths, HttpStatus.OK);
  }

  @Override
  public ResponseEntity<DRSObject> postObject(
      @PathVariable("object_id") String objectId,
      @RequestBody DRSPassportRequestModel drsPassportRequestModel) {
    DRSObject drsObject = drsService.lookupObjectByDrsIdPassport(objectId, drsPassportRequestModel);
    return new ResponseEntity<>(drsObject, HttpStatus.OK);
  }

  /*
   * WARNING: if making any changes to this method make sure to notify the #dsp-batch channel! Describe the change and
   * any consequences downstream to DRS clients.
   */
  @Override
  public ResponseEntity<DRSServiceInfo> getServiceInfo() {
    DRSServiceInfo info =
        new DRSServiceInfo()
            .version("0.0.1")
            .title("Terra Data Repository")
            .description(
                "Terra Data Repository (Jade) - a Broad/Verily/Microsoft open source project")
            .contact("cbernard@broadinstitute.org")
            .license("Apache 2.0");
    return new ResponseEntity<>(info, HttpStatus.OK);
  }
}
