package bio.terra.app.controller;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.utils.ControllerUtils;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.controller.AdminApi;
import bio.terra.model.DrsAliasModel;
import bio.terra.model.JobModel;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.filedata.DrsService;
import bio.terra.service.job.JobService;
import bio.terra.service.resourcemanagement.azure.AzureMonitoringService;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountService;
import io.swagger.annotations.Api;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;

@Controller
@Api(tags = {"admin"})
public class AdminApiController implements AdminApi {

  private final HttpServletRequest request;
  private final JobService jobService;
  private final AuthenticatedUserRequestFactory authenticatedUserRequestFactory;
  private final DrsService drsService;
  private final IamService iamService;
  private final ApplicationConfiguration appConfig;
  private final AzureStorageAccountService storageAccountService;
  private final AzureMonitoringService monitoringService;

  @Autowired
  public AdminApiController(
      HttpServletRequest request,
      JobService jobService,
      AuthenticatedUserRequestFactory authenticatedUserRequestFactory,
      DrsService drsService,
      IamService iamService,
      AzureStorageAccountService storageAccountService,
      AzureMonitoringService monitoringService,
      ApplicationConfiguration appConfig) {
    this.request = request;
    this.jobService = jobService;
    this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
    this.drsService = drsService;
    this.iamService = iamService;
    this.monitoringService = monitoringService;
    this.storageAccountService = storageAccountService;
    this.appConfig = appConfig;
  }

  private AuthenticatedUserRequest getAuthenticatedInfo() {
    return authenticatedUserRequestFactory.from(request);
  }

  @Override
  public ResponseEntity<JobModel> registerDrsAliases(List<DrsAliasModel> aliases) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    // Make sure the user is an admin by checking for configure action
    iamService.verifyAuthorization(
        userReq, IamResourceType.DATAREPO, appConfig.getResourceId(), IamAction.CONFIGURE);
    String jobId = drsService.registerDrsAliases(aliases, userReq);
    return ControllerUtils.jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  //  @Override
  //  public ResponseEntity<JobModel> cleanupAzureStorageAccount(
  //      String subscriptionId, String resourceGroupName, String storageAccountName) {
  //    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
  //    // Make sure the user is an admin by checking for configure action
  //    iamService.verifyAuthorization(
  //        userReq, IamResourceType.DATAREPO, appConfig.getResourceId(), IamAction.CONFIGURE);
  //    String jobId =
  //        storageAccountService.storageAccountCleanup(
  //            UUID.fromString(subscriptionId), resourceGroupName, storageAccountName, userReq);
  //    return ControllerUtils.jobToResponse(jobService.retrieveJob(jobId, userReq));
  //  }
}
