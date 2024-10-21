package bio.terra.app.controller;

import bio.terra.app.configuration.ApplicationConfiguration;
import bio.terra.app.utils.ControllerUtils;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.common.iam.AuthenticatedUserRequestFactory;
import bio.terra.controller.AdminApi;
import bio.terra.model.DatasetModel;
import bio.terra.model.DatasetRequestAccessIncludeModel;
import bio.terra.model.DrsAliasModel;
import bio.terra.model.JobModel;
import bio.terra.model.SnapshotModel;
import bio.terra.model.SnapshotRetrieveIncludeModel;
import bio.terra.service.auth.iam.IamAction;
import bio.terra.service.auth.iam.IamResourceType;
import bio.terra.service.auth.iam.IamService;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.filedata.DrsService;
import bio.terra.service.job.JobService;
import bio.terra.service.snapshot.SnapshotService;
import io.swagger.annotations.Api;
import jakarta.servlet.http.HttpServletRequest;
import java.util.List;
import java.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
  private final DatasetService datasetService;
  private final SnapshotService snapshotService;
  private final ApplicationConfiguration appConfig;
  private static final Logger logger = LoggerFactory.getLogger(AdminApiController.class);

  @Autowired
  public AdminApiController(
      HttpServletRequest request,
      JobService jobService,
      AuthenticatedUserRequestFactory authenticatedUserRequestFactory,
      DrsService drsService,
      IamService iamService,
      DatasetService datasetService,
      SnapshotService snapshotService,
      ApplicationConfiguration appConfig) {
    this.request = request;
    this.jobService = jobService;
    this.authenticatedUserRequestFactory = authenticatedUserRequestFactory;
    this.drsService = drsService;
    this.iamService = iamService;
    this.datasetService = datasetService;
    this.snapshotService = snapshotService;
    this.appConfig = appConfig;
  }

  private AuthenticatedUserRequest getAuthenticatedInfo() {
    return authenticatedUserRequestFactory.from(request);
  }

  @Override
  public ResponseEntity<JobModel> registerDrsAliases(List<DrsAliasModel> aliases) {
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    iamService.verifyAuthorization(
        userReq,
        IamResourceType.DATAREPO,
        appConfig.getResourceId(),
        IamAction.REGISTER_DRS_ALIASES);
    String jobId = drsService.registerDrsAliases(aliases, userReq);
    return ControllerUtils.jobToResponse(jobService.retrieveJob(jobId, userReq));
  }

  @Override
  public ResponseEntity<DatasetModel> adminRetrieveDataset(UUID id) {
    List<DatasetRequestAccessIncludeModel> include =
        List.of(
            DatasetRequestAccessIncludeModel.DATA_PROJECT,
            DatasetRequestAccessIncludeModel.PROFILE,
            DatasetRequestAccessIncludeModel.PROPERTIES,
            DatasetRequestAccessIncludeModel.SCHEMA,
            DatasetRequestAccessIncludeModel.STORAGE);
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    logger.info(
        "Verifying resource type admin authorization: {} for resource type: {} and resource id: {}",
        userReq.getEmail(),
        IamResourceType.DATASET,
        id);
    iamService.verifyResourceTypeAdminAuthorized(
        userReq, IamResourceType.DATASET, IamAction.ADMIN_READ_SUMMARY_INFORMATION);
    logger.info("Retrieving dataset id: {}", id);
    DatasetModel datasetModel = datasetService.retrieveDatasetModel(id, userReq, include);
    return ResponseEntity.ok(datasetModel);
  }

  @Override
  public ResponseEntity<SnapshotModel> adminRetrieveSnapshot(UUID id) {
    List<SnapshotRetrieveIncludeModel> include =
        List.of(
            SnapshotRetrieveIncludeModel.SOURCES,
            SnapshotRetrieveIncludeModel.TABLES,
            SnapshotRetrieveIncludeModel.RELATIONSHIPS,
            SnapshotRetrieveIncludeModel.PROFILE,
            SnapshotRetrieveIncludeModel.PROPERTIES,
            SnapshotRetrieveIncludeModel.DATA_PROJECT,
            SnapshotRetrieveIncludeModel.CREATION_INFORMATION,
            SnapshotRetrieveIncludeModel.DUOS);
    AuthenticatedUserRequest userReq = getAuthenticatedInfo();
    logger.info(
        "Verifying resource type admin authorization: {} for resource type: {} and resource id: {}",
        userReq.getEmail(),
        IamResourceType.DATASNAPSHOT,
        id);
    iamService.verifyResourceTypeAdminAuthorized(
        userReq, IamResourceType.DATASNAPSHOT, IamAction.ADMIN_READ_SUMMARY_INFORMATION);
    logger.info("Retrieving snapshot id: {}", id);
    SnapshotModel snapshotModel = snapshotService.retrieveSnapshotModel(id, include, userReq);
    return ResponseEntity.ok(snapshotModel);
  }
}
