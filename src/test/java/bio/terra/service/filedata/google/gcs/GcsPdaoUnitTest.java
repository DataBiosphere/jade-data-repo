package bio.terra.service.filedata.google.gcs;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.app.logging.PerformanceLogger;
import bio.terra.common.category.Unit;
import bio.terra.common.exception.PdaoException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.CloudPlatform;
import bio.terra.service.auth.iam.IamProviderInterface;
import bio.terra.service.auth.oauth2.GoogleOauthUtils;
import bio.terra.service.configuration.ConfigurationService;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetSummary;
import bio.terra.service.filedata.FileIdService;
import bio.terra.service.filedata.exception.BlobAccessNotAuthorizedException;
import bio.terra.service.filedata.google.firestore.FireStoreDao;
import bio.terra.service.resourcemanagement.ResourceService;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectService;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import bio.terra.service.resourcemanagement.google.GoogleResourceManagerService;
import com.google.api.services.oauth2.model.Tokeninfo;
import com.google.auth.oauth2.OAuth2Credentials;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageException;
import com.google.cloud.storage.StorageOptions;
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

  @Test
  public void testValidateUserCanReadWithNullCloudEncapsulationId() throws InterruptedException {
    // Create the Tokeninfo object that getOauth2TokenInfo should return
    Tokeninfo tokeninfo = new Tokeninfo();
    tokeninfo.setEmail("pet-sa@test-project.iam.gserviceaccount.com");
    tokeninfo.setExpiresIn(3600);

    try (MockedStatic<GoogleOauthUtils> mockedOauth = mockStatic(GoogleOauthUtils.class);
        MockedStatic<StorageOptions> mockedStorageOptions = mockStatic(StorageOptions.class)) {
      when(environment.getActiveProfiles()).thenReturn(new String[] {"google"});
      Dataset dataset =
          new Dataset(new DatasetSummary().cloudPlatform(CloudPlatform.GCP))
              .id(UUID.randomUUID())
              .name("test_dataset");

      List<String> sourcePaths = List.of("gs://test-bucket/test-file.txt");
      String userToken = "oauthToken";

      when(iamClient.getPetToken(TEST_USER, GCS_VERIFICATION_SCOPES)).thenReturn(userToken);

      // Mock the static method call
      mockedOauth.when(() -> GoogleOauthUtils.getOauth2TokenInfo(userToken)).thenReturn(tokeninfo);

      // Mock testIamPermissions to return true
      Storage mockStorage = getMockStorage(mockedStorageOptions, false);
      when(mockStorage.testIamPermissions(
              anyString(), anyList(), any(Storage.BucketSourceOption[].class)))
          .thenReturn(List.of(true));

      // Test with null cloudEncapsulationId - no error, should use user token as key
      gcsPdao.validateUserCanRead(sourcePaths, null, TEST_USER, dataset);
    }
  }

  @Test
  public void testValidateUserCanReadWithCloudEncapsulationId() throws InterruptedException {
    // Create the Tokeninfo object that getOauth2TokenInfo should return
    Tokeninfo tokeninfo = new Tokeninfo();
    tokeninfo.setEmail("pet-sa@test-project.iam.gserviceaccount.com");
    tokeninfo.setExpiresIn(3600);

    try (MockedStatic<GoogleOauthUtils> mockedOauth = mockStatic(GoogleOauthUtils.class);
        MockedStatic<StorageOptions> mockedStorageOptions = mockStatic(StorageOptions.class)) {
      when(environment.getActiveProfiles()).thenReturn(new String[] {"google"});
      Dataset dataset =
          new Dataset(new DatasetSummary().cloudPlatform(CloudPlatform.GCP))
              .id(UUID.randomUUID())
              .name("test_dataset");

      List<String> sourcePaths = List.of("gs://test-bucket/test-file.txt");
      String userToken = "oauthToken";
      String cloudEncapsulationId = "requester-pays-project";

      when(iamClient.getPetToken(TEST_USER, GCS_VERIFICATION_SCOPES)).thenReturn(userToken);
      doNothing()
          .when(googleResourceManagerService)
          .updateIamPermissions(
              Map.of(
                  GCS_REQUESTER_PAYS_TARGET_ROLE,
                  List.of("serviceAccount:" + tokeninfo.getEmail())),
              cloudEncapsulationId,
              GoogleProjectService.PermissionOp.ENABLE_PERMISSIONS);

      // Mock the static method call
      mockedOauth.when(() -> GoogleOauthUtils.getOauth2TokenInfo(userToken)).thenReturn(tokeninfo);

      // Mock testIamPermissions to return true
      Storage mockStorage = getMockStorage(mockedStorageOptions, true);
      when(mockStorage.testIamPermissions(
              anyString(), anyList(), any(Storage.BucketSourceOption[].class)))
          .thenReturn(List.of(true));

      // Test with cloudEncapsulationId - should use combined token key
      gcsPdao.validateUserCanRead(sourcePaths, cloudEncapsulationId, TEST_USER, dataset);
    }
  }

  @Test
  public void testValidateUserCanReadAzureDatasetSkipsPetAccountSetup()
      throws InterruptedException {
    // Create the Tokeninfo object that getOauth2TokenInfo should return
    Tokeninfo tokeninfo = new Tokeninfo();
    tokeninfo.setEmail("pet-sa@test-project.iam.gserviceaccount.com");
    tokeninfo.setExpiresIn(3600);

    try (MockedStatic<GoogleOauthUtils> mockedOauth = mockStatic(GoogleOauthUtils.class);
        MockedStatic<StorageOptions> mockedStorageOptions = mockStatic(StorageOptions.class)) {
      when(environment.getActiveProfiles()).thenReturn(new String[] {"google"});
      // Create Azure dataset - should skip GCP-specific pet account setup
      Dataset dataset =
          new Dataset(new DatasetSummary().cloudPlatform(CloudPlatform.AZURE))
              .id(UUID.randomUUID())
              .name("test_dataset");

      List<String> sourcePaths = List.of("gs://test-bucket/test-file.txt");
      String userToken = "oauthToken";

      when(iamClient.getPetToken(TEST_USER, GCS_VERIFICATION_SCOPES)).thenReturn(userToken);

      // Mock the static method call
      mockedOauth.when(() -> GoogleOauthUtils.getOauth2TokenInfo(userToken)).thenReturn(tokeninfo);

      // Mock testIamPermissions to return true
      Storage mockStorage = getMockStorage(mockedStorageOptions, false);
      when(mockStorage.testIamPermissions(
              anyString(), anyList(), any(Storage.BucketSourceOption[].class)))
          .thenReturn(List.of(true));

      // Test with Azure dataset - should not call addPetServiceAccountToDatasetProject
      // so don't need to mock resourceManagerService
      gcsPdao.validateUserCanRead(sourcePaths, projectId, TEST_USER, dataset);
    }
  }

  @Test
  public void testValidateUserCanReadMultipleBucketsDeduplication() throws InterruptedException {
    // Create the Tokeninfo object that getOauth2TokenInfo should return
    Tokeninfo tokeninfo = new Tokeninfo();
    tokeninfo.setEmail("pet-sa@test-project.iam.gserviceaccount.com");
    tokeninfo.setExpiresIn(3600);

    try (MockedStatic<GoogleOauthUtils> mockedOauth = mockStatic(GoogleOauthUtils.class);
        MockedStatic<StorageOptions> mockedStorageOptions = mockStatic(StorageOptions.class)) {
      when(environment.getActiveProfiles()).thenReturn(new String[] {"google"});
      Dataset dataset =
          new Dataset(new DatasetSummary().cloudPlatform(CloudPlatform.GCP))
              .id(UUID.randomUUID())
              .name("test_dataset");

      // Multiple paths from same buckets
      List<String> sourcePaths =
          List.of(
              "gs://bucket1/file1.txt",
              "gs://bucket1/file2.txt",
              "gs://bucket2/file3.txt",
              "gs://bucket2/subfolder/file4.txt");
      String userToken = "oauthToken";

      when(iamClient.getPetToken(TEST_USER, GCS_VERIFICATION_SCOPES)).thenReturn(userToken);
      doNothing()
          .when(googleResourceManagerService)
          .updateIamPermissions(
              Map.of(
                  GCS_REQUESTER_PAYS_TARGET_ROLE,
                  List.of("serviceAccount:" + tokeninfo.getEmail())),
              projectId,
              GoogleProjectService.PermissionOp.ENABLE_PERMISSIONS);

      // Mock the static method call
      mockedOauth.when(() -> GoogleOauthUtils.getOauth2TokenInfo(userToken)).thenReturn(tokeninfo);

      // Mock testIamPermissions to return true
      Storage mockStorage = getMockStorage(mockedStorageOptions, false);
      when(mockStorage.testIamPermissions(
              anyString(), anyList(), any(Storage.BucketSourceOption[].class)))
          .thenReturn(List.of(true));

      // Test with multiple paths from same bucket, should test each unique bucket only once
      gcsPdao.validateUserCanRead(sourcePaths, projectId, TEST_USER, dataset);
      verify(mockStorage, times(1)).testIamPermissions(eq("bucket1"), any(), any());
      verify(mockStorage, times(1)).testIamPermissions(eq("bucket2"), any(), any());
    }
  }

  @Test
  public void testValidateUserCanReadInterruptedExceptionThrowsPdaoException()
      throws InterruptedException {
    when(environment.getActiveProfiles()).thenReturn(new String[] {"google"});
    Dataset dataset =
        new Dataset(new DatasetSummary().cloudPlatform(CloudPlatform.GCP))
            .id(UUID.randomUUID())
            .name("test_dataset");

    List<String> sourcePaths = List.of("gs://test-bucket/test-file.txt");

    // Mock getPetToken to throw InterruptedException
    when(iamClient.getPetToken(TEST_USER, GCS_VERIFICATION_SCOPES))
        .thenThrow(new InterruptedException("Thread interrupted"));

    // Should throw PdaoException wrapping the InterruptedException
    assertThrows(
        PdaoException.class,
        () -> gcsPdao.validateUserCanRead(sourcePaths, projectId, TEST_USER, dataset));
  }

  @Test
  public void testValidateUserCanReadIamUnauthorizedExceptionThrowsPdaoException()
      throws InterruptedException {
    when(environment.getActiveProfiles()).thenReturn(new String[] {"google"});
    Dataset dataset =
        new Dataset(new DatasetSummary().cloudPlatform(CloudPlatform.GCP))
            .id(UUID.randomUUID())
            .name("test_dataset");

    List<String> sourcePaths = List.of("gs://test-bucket/test-file.txt");

    // Mock getPetToken to throw IamUnauthorizedException
    when(iamClient.getPetToken(TEST_USER, GCS_VERIFICATION_SCOPES))
        .thenThrow(
            new bio.terra.service.auth.iam.exception.IamUnauthorizedException("Unauthorized"));

    // Should throw PdaoException with message about token timeout
    PdaoException exception =
        assertThrows(
            PdaoException.class,
            () -> gcsPdao.validateUserCanRead(sourcePaths, projectId, TEST_USER, dataset));

    // Verify the exception contains the message about token timeout
    assertTrue(exception.getCauses().get(0).contains("user request token may have timed out"));
  }

  @Test
  public void testValidateUserCanReadStorageException() throws InterruptedException {
    // Create the Tokeninfo object that getOauth2TokenInfo should return
    Tokeninfo tokeninfo = new Tokeninfo();
    tokeninfo.setEmail("pet-sa@test-project.iam.gserviceaccount.com");
    tokeninfo.setExpiresIn(3600);

    try (MockedStatic<GoogleOauthUtils> mockedOauth = mockStatic(GoogleOauthUtils.class);
        MockedStatic<StorageOptions> mockedStorageOptions = mockStatic(StorageOptions.class)) {
      when(environment.getActiveProfiles()).thenReturn(new String[] {"google"});
      Dataset dataset =
          new Dataset(new DatasetSummary().cloudPlatform(CloudPlatform.GCP))
              .id(UUID.randomUUID())
              .name("test_dataset");

      List<String> sourcePaths = List.of("gs://test-bucket/test-file.txt");
      String userToken = "oauthToken";

      when(iamClient.getPetToken(TEST_USER, GCS_VERIFICATION_SCOPES)).thenReturn(userToken);
      doNothing()
          .when(googleResourceManagerService)
          .updateIamPermissions(
              Map.of(
                  GCS_REQUESTER_PAYS_TARGET_ROLE,
                  List.of("serviceAccount:" + tokeninfo.getEmail())),
              projectId,
              GoogleProjectService.PermissionOp.ENABLE_PERMISSIONS);

      // Mock the static method call
      mockedOauth.when(() -> GoogleOauthUtils.getOauth2TokenInfo(userToken)).thenReturn(tokeninfo);

      // Mock StorageOptions builder chain
      Storage mockStorage = getMockStorage(mockedStorageOptions, true);

      // Mock testIamPermissions to throw StorageException
      when(mockStorage.testIamPermissions(
              anyString(), anyList(), any(Storage.BucketSourceOption[].class)))
          .thenThrow(new StorageException(403, "Forbidden"));

      // Should throw StorageException with detailed message
      StorageException exception =
          assertThrows(
              StorageException.class,
              () -> gcsPdao.validateUserCanRead(sourcePaths, projectId, TEST_USER, dataset));

      // Verify the exception contains the expected details
      assertTrue(exception.getMessage().contains("Could not test permissions"));
      assertTrue(exception.getMessage().contains("test-bucket"));
      assertTrue(exception.getMessage().contains(tokeninfo.getEmail()));
    }
  }

  @Test
  public void testValidateUserCanReadBlobAccessNotAuthorizedException()
      throws InterruptedException {
    // Create the Tokeninfo object that getOauth2TokenInfo should return
    Tokeninfo tokeninfo = new Tokeninfo();
    tokeninfo.setEmail("pet-sa@test-project.iam.gserviceaccount.com");
    tokeninfo.setExpiresIn(3600);

    try (MockedStatic<GoogleOauthUtils> mockedOauth = mockStatic(GoogleOauthUtils.class);
        MockedStatic<StorageOptions> mockedStorageOptions = mockStatic(StorageOptions.class)) {
      when(environment.getActiveProfiles()).thenReturn(new String[] {"google"});
      Dataset dataset =
          new Dataset(new DatasetSummary().cloudPlatform(CloudPlatform.GCP))
              .id(UUID.randomUUID())
              .name("test_dataset")
              .projectResource(new GoogleProjectResource());

      List<String> sourcePaths = List.of("gs://test-bucket/test-file.txt");
      String userToken = "oauthToken";
      String proxyGroup = "proxy-group@test.com";

      when(iamClient.getPetToken(TEST_USER, GCS_VERIFICATION_SCOPES)).thenReturn(userToken);
      when(iamClient.getProxyGroup(TEST_USER)).thenReturn(proxyGroup);
      doNothing()
          .when(googleResourceManagerService)
          .updateIamPermissions(
              Map.of(
                  GCS_REQUESTER_PAYS_TARGET_ROLE,
                  List.of("serviceAccount:" + tokeninfo.getEmail())),
              projectId,
              GoogleProjectService.PermissionOp.ENABLE_PERMISSIONS);

      // Mock the static method call
      mockedOauth.when(() -> GoogleOauthUtils.getOauth2TokenInfo(userToken)).thenReturn(tokeninfo);

      // Mock testIamPermissions to return false (insufficient permissions)
      Storage mockStorage = getMockStorage(mockedStorageOptions, true);
      when(mockStorage.testIamPermissions(
              anyString(), anyList(), any(Storage.BucketSourceOption[].class)))
          .thenReturn(List.of(false));

      // Should throw BlobAccessNotAuthorizedException
      BlobAccessNotAuthorizedException exception =
          assertThrows(
              BlobAccessNotAuthorizedException.class,
              () -> gcsPdao.validateUserCanRead(sourcePaths, projectId, TEST_USER, dataset));

      // Verify the exception contains expected details about insufficient permissions
      assertTrue(exception.getMessage().contains("is not authorized for user"));
      assertTrue(exception.getMessage().contains("test-bucket"));
      assertTrue(exception.getMessage().contains(proxyGroup));
    }
  }

  private static Storage getMockStorage(
      MockedStatic<StorageOptions> mockedStorageOptions, boolean projectId) {
    StorageOptions.Builder mockBuilder = mock(StorageOptions.Builder.class);
    StorageOptions mockStorageOptions = mock(StorageOptions.class);
    Storage mockStorage = mock(Storage.class);

    mockedStorageOptions.when(StorageOptions::newBuilder).thenReturn(mockBuilder);
    when(mockBuilder.setCredentials(any(OAuth2Credentials.class))).thenReturn(mockBuilder);
    if (projectId) {
      when(mockBuilder.setProjectId(anyString())).thenReturn(mockBuilder);
    }
    when(mockBuilder.build()).thenReturn(mockStorageOptions);
    when(mockStorageOptions.getService()).thenReturn(mockStorage);
    return mockStorage;
  }
}
