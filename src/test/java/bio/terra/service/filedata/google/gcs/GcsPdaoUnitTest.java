package bio.terra.service.filedata.google.gcs;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.when;

import bio.terra.app.logging.PerformanceLogger;
import bio.terra.common.category.Unit;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.CloudPlatform;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.auth.oauth2.GoogleOauthUtils;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.filedata.FileIdService;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import bio.terra.service.resourcemanagement.google.GoogleResourceManagerService;
import com.google.api.services.oauth2.model.Tokeninfo;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.core.env.Environment;
import org.springframework.test.context.bean.override.mockito.MockitoBean;

@Tag(Unit.TAG)
@ExtendWith(MockitoExtension.class)
class GcsPdaoUnitTest {

  @MockitoBean private GoogleResourceDao googleResourceDao;
  private final Environment environment = mock(Environment.class);
  private final IamProviderInterface iamClient = mock(IamProviderInterface.class);
  private final GoogleResourceManagerService googleResourceManagerService =
      mock(GoogleResourceManagerService.class);
  @MockitoBean private ResourceService resourceService;
  @MockitoBean private GcsProjectFactory gcsProjectFactory;
  @MockitoBean private FireStoreDao fileDao;
  @MockitoBean private ConfigurationService configurationService;
  @MockitoBean private ExecutorService executor;
  @MockitoBean private PerformanceLogger performanceLogger;
  @MockitoBean private FileIdService fileIdService;
  private GcsPdao gcsPdao;

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticatedUserRequest.builder()
          .setSubjectId("DatasetUnit")
          .setEmail("dataset@unit.com")
          .setToken("token")
          .build();
  private final String projectId = "project-id";
  private static final List<String> GCS_VERIFICATION_SCOPES =
      List.of(
          "openid", "email", "profile", "https://www.googleapis.com/auth/devstorage.full_control");
  private static final String GCS_REQUESTER_PAYS_TARGET_ROLE =
      "roles/serviceusage.serviceUsageConsumer";

  @BeforeEach
  public void setUp() throws Exception {
    gcsPdao =
        new GcsPdao(
            gcsProjectFactory,
            resourceService,
            fileDao,
            configurationService,
            executor,
            performanceLogger,
            iamClient,
            environment,
            googleResourceManagerService,
            "test-service-account@test-project.iam.gserviceaccount.com",
            fileIdService);
  }

  @Test
  public void testValidateUserCanReadWithConnectedTestProfile() {
    // mock that this is a connected test env
    when(environment.getActiveProfiles()).thenReturn(new String[] {"google", "connectedtest"});
    // Test that validation is skipped in connectedtest profile
    Dataset dataset = new Dataset().id(UUID.randomUUID()).name("test_dataset");

    List<String> sourcePaths = List.of("gs://test-bucket/test-file.txt");

    // Should not throw any exception since connectedtest profile is active
    gcsPdao.validateUserCanRead(sourcePaths, projectId, TEST_USER, dataset);
  }

  @Test
  public void testValidateUserCanReadWithDedicatedServiceAccount() {
    when(environment.getActiveProfiles()).thenReturn(new String[] {"google"});
    // Test that validation is skipped for datasets with dedicated service accounts
    Dataset dataset =
        new Dataset()
            .id(UUID.randomUUID())
            .name("test_dataset")
            .projectResource(new GoogleProjectResource().dedicatedServiceAccount(true));

    List<String> sourcePaths = List.of("gs://test-bucket/test-file.txt");

    // Should not throw any exception since validation is skipped
    gcsPdao.validateUserCanRead(sourcePaths, projectId, TEST_USER, dataset);
  }

  @Test
  public void testValidateUserCanReadWithEmptySourcePaths() throws InterruptedException {
    // Create the Tokeninfo object that getOauth2TokenInfo should return
    Tokeninfo tokeninfo = new Tokeninfo();
    tokeninfo.setEmail("pet-sa@test-project.iam.gserviceaccount.com");
    tokeninfo.setExpiresIn(3600);
    try (MockedStatic<GoogleOauthUtils> mockedOauth = mockStatic(GoogleOauthUtils.class)) {
      when(environment.getActiveProfiles()).thenReturn(new String[] {"google"});
      Dataset dataset =
          new Dataset(new DatasetSummary().cloudPlatform(CloudPlatform.GCP))
              .id(UUID.randomUUID())
              .name("test_dataset");

      List<String> sourcePaths = List.of();

      String userToken = "oauthToken";
      when(iamClient.getPetToken(TEST_USER, GCS_VERIFICATION_SCOPES)).thenReturn(userToken);
      doNothing()
          .when(googleResourceManagerService)
          .updateIamPermissions(
              Map.of(GCS_REQUESTER_PAYS_TARGET_ROLE, List.of(tokeninfo.getEmail())),
              projectId,
              GoogleProjectService.PermissionOp.ENABLE_PERMISSIONS);
      // Configure the mock to return tokeninfo when called with petToken
      mockedOauth.when(() -> GoogleOauthUtils.getOauth2TokenInfo(userToken)).thenReturn(tokeninfo);
      // Should not throw any exception with empty source paths
      gcsPdao.validateUserCanRead(sourcePaths, projectId, TEST_USER, dataset);
    }
  }
}
