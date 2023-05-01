package bio.terra.service.filedata.azure.blobstore;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Unit;
import bio.terra.common.exception.PdaoException;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.FileLoadModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FSFileInfo;
import bio.terra.service.filedata.azure.util.BlobContainerClientFactory;
import bio.terra.service.filedata.azure.util.BlobContainerCopier;
import bio.terra.service.filedata.azure.util.BlobContainerCopyInfo;
import bio.terra.service.filedata.azure.util.BlobContainerCopySyncPoller;
import bio.terra.service.filedata.azure.util.BlobCrl;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.filedata.google.gcs.GcsPdao;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureContainerPdao;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import bio.terra.service.resourcemanagement.azure.AzureResourceDao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import com.azure.core.credential.TokenCredential;
import com.azure.core.util.polling.LongRunningOperationStatus;
import com.azure.core.util.polling.PollResponse;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.blob.models.BlobCopyInfo;
import com.azure.storage.blob.models.BlobProperties;
import com.google.cloud.storage.Blob;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
@EmbeddedDatabaseTest
public class AzureBlobStorePdaoTest {
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticatedUserRequest.builder()
          .setSubjectId("DatasetUnit")
          .setEmail("dataset@unit.com")
          .setToken("token")
          .build();
  private static final UUID PROFILE_ID = UUID.randomUUID();
  private static final UUID RESOURCE_ID = UUID.randomUUID();
  private static final UUID TENANT_ID = UUID.randomUUID();
  private static final String STORAGE_ACCOUNT_NAME = "sa";
  private static final BillingProfileModel BILLING_PROFILE =
      new BillingProfileModel().id(PROFILE_ID).tenantId(TENANT_ID);
  private static final AzureStorageAccountResource AZURE_STORAGE_ACCOUNT_RESOURCE =
      new AzureStorageAccountResource()
          .resourceId(RESOURCE_ID)
          .profileId(PROFILE_ID)
          .name(STORAGE_ACCOUNT_NAME)
          .dataContainer("d")
          .metadataContainer("md");
  private static final String SOURCE_CONTAINER_NAME = "srcdata";
  private static final String SOURCE_FILE_NAME = "src.txt";
  private static final String SOURCE_BLOB_NAME = SOURCE_CONTAINER_NAME + "/" + SOURCE_FILE_NAME;
  private static final String SOURCE_PATH = "https://src.blob.core.windows.net/" + SOURCE_BLOB_NAME;
  private static final String SOURCE_GCS_PATH = "gs://mybucket/" + SOURCE_BLOB_NAME;
  private static final String TARGET_PATH = "/foo/bar.txt";
  private static final OffsetDateTime BLOB_CREATION_TIME = OffsetDateTime.now();
  private static final long BLOB_SIZE = 1234567890L;
  private static final byte[] BLOB_CONTENT_MD5 = "FOOBAR".getBytes(StandardCharsets.UTF_8);
  // This is bytes of the FOOBAR string in Hex
  private static final String BLOB_CONTENT_MD5_HEX = "464f4f424152";
  private static final String LOAD_TAG = "tag";
  private static final String MIME_TYPE = "txt/plain";

  private BlobContainerClientFactory sourceBlobContainerFactory;
  private BlobContainerClientFactory targetBlobContainerFactory;
  private BlobCrl blobCrl;
  @MockBean private ProfileDao profileDao;
  @MockBean private AzureContainerPdao azureContainerPdao;
  @MockBean private AzureResourceConfiguration resourceConfiguration;
  @MockBean private AzureResourceDao azureResourceDao;
  @MockBean private AzureAuthService azureAuthService;
  @MockBean private GcsPdao gcsPdao;
  @Autowired private AzureBlobStorePdao dao;

  @MockBean
  @Qualifier("synapseJdbcTemplate")
  private NamedParameterJdbcTemplate synapseJdbcTemplate;

  private FileLoadModel fileLoadModel;

  @Before
  public void setUp() {
    dao = spy(dao);

    TokenCredential targetCredential = mock(TokenCredential.class);
    fileLoadModel =
        new FileLoadModel()
            .profileId(PROFILE_ID)
            .targetPath(TARGET_PATH)
            .loadTag(LOAD_TAG)
            .mimeType(MIME_TYPE);
    when(profileDao.getBillingProfileById(PROFILE_ID)).thenReturn(BILLING_PROFILE);
    when(resourceConfiguration.getAppToken(TENANT_ID)).thenReturn(targetCredential);
    when(resourceConfiguration.getMaxRetries()).thenReturn(3);
    when(resourceConfiguration.getRetryTimeoutSeconds()).thenReturn(60);
    when(azureResourceDao.retrieveStorageAccountById(RESOURCE_ID))
        .thenReturn(AZURE_STORAGE_ACCOUNT_RESOURCE);
    targetBlobContainerFactory = mock(BlobContainerClientFactory.class);
    sourceBlobContainerFactory = mock(BlobContainerClientFactory.class);
    blobCrl = mock(BlobCrl.class);
    doReturn(targetBlobContainerFactory).when(dao).getTargetDataClientFactory(any(), any(), any());
    doReturn(sourceBlobContainerFactory)
        .when(dao)
        .getSourceClientFactory(anyString(), any(), anyString());
    doReturn(sourceBlobContainerFactory).when(dao).getSourceClientFactory(any());
    doReturn(blobCrl).when(dao).getBlobCrl(any());
  }

  @Test
  public void testCopyFile() {
    UUID fileId = UUID.randomUUID();
    fileLoadModel.sourcePath(SOURCE_PATH);

    FSFileInfo expectedFileInfo = mockFileCopy(fileId);

    FSFileInfo fsFileInfo =
        dao.copyFile(
            new Dataset().id(UUID.randomUUID()).predictableFileIds(false),
            BILLING_PROFILE,
            fileLoadModel,
            fileId.toString(),
            AZURE_STORAGE_ACCOUNT_RESOURCE,
            TEST_USER);
    assertThat("output is expected", fsFileInfo, samePropertyValuesAs(expectedFileInfo));
  }

  @Test
  public void testCopyFileWithSas() {
    UUID fileId = UUID.randomUUID();
    fileLoadModel.sourcePath(
        SOURCE_PATH
            + "?sp=r&st=2021-07-14T19:31:16Z&se=2021-07-15T03:31:16Z&"
            + "spr=https&sv=2020-08-04&sr=b&sig=mysig");

    FSFileInfo expectedFileInfo = mockFileCopy(fileId);

    FSFileInfo fsFileInfo =
        dao.copyFile(
            new Dataset().id(UUID.randomUUID()).predictableFileIds(false),
            BILLING_PROFILE,
            fileLoadModel,
            fileId.toString(),
            AZURE_STORAGE_ACCOUNT_RESOURCE,
            TEST_USER);
    assertThat("output is expected", fsFileInfo, samePropertyValuesAs(expectedFileInfo));
  }

  @Test
  public void testCopyFileWithGcsFile() {
    UUID fileId = UUID.randomUUID();
    fileLoadModel.sourcePath(SOURCE_GCS_PATH);

    FSFileInfo expectedFileInfo = mockFileCopy(fileId);

    FSFileInfo fsFileInfo =
        dao.copyFile(
            new Dataset().id(UUID.randomUUID()).predictableFileIds(false),
            BILLING_PROFILE,
            fileLoadModel,
            fileId.toString(),
            AZURE_STORAGE_ACCOUNT_RESOURCE,
            TEST_USER);
    assertThat("output is expected", fsFileInfo, samePropertyValuesAs(expectedFileInfo));
    verify(gcsPdao, times(1)).getBlobFromGsPathNs(any(), eq(SOURCE_GCS_PATH), eq(null));
  }

  @Test
  public void testCopyFileWithGcsFileAndProject() {
    UUID fileId = UUID.randomUUID();
    String userProject = "foo";
    String sourcePath = SOURCE_GCS_PATH + "?userProject=" + userProject;
    fileLoadModel.sourcePath(sourcePath);

    FSFileInfo expectedFileInfo = mockFileCopy(fileId);

    FSFileInfo fsFileInfo =
        dao.copyFile(
            new Dataset().id(UUID.randomUUID()).predictableFileIds(false),
            BILLING_PROFILE,
            fileLoadModel,
            fileId.toString(),
            AZURE_STORAGE_ACCOUNT_RESOURCE,
            TEST_USER);
    assertThat("output is expected", fsFileInfo, samePropertyValuesAs(expectedFileInfo));
    verify(gcsPdao, times(1)).getBlobFromGsPathNs(any(), eq(SOURCE_GCS_PATH), eq(userProject));
  }

  @Test
  public void testDeleteFile() {
    UUID fileId = UUID.randomUUID();
    FSFileInfo fsFileInfo = mockFileCopy(fileId);
    when(blobCrl.deleteBlob("data/" + fileId + "/" + SOURCE_FILE_NAME)).thenReturn(true);

    FireStoreFile fileToDelete =
        new FireStoreFile()
            .fileId(fileId.toString())
            .bucketResourceId(RESOURCE_ID.toString())
            .gspath(fsFileInfo.getCloudPath());
    assertThat(dao.deleteFile(fileToDelete, TEST_USER), equalTo(true));
  }

  @Test
  public void testDeleteFileNotFound() {
    UUID fileId = UUID.randomUUID();
    FSFileInfo fsFileInfo = mockFileCopy(fileId);
    when(blobCrl.deleteBlob("data/" + fileId + "/" + SOURCE_FILE_NAME)).thenReturn(false);

    FireStoreFile fileToDelete =
        new FireStoreFile()
            .fileId(fileId.toString())
            .bucketResourceId(RESOURCE_ID.toString())
            .gspath(fsFileInfo.getCloudPath());
    assertThat(dao.deleteFile(fileToDelete, TEST_USER), equalTo(false));
  }

  @Test
  public void testDeleteFileMismatchedStorageAccount() {
    UUID fileId = UUID.randomUUID();
    mockFileCopy(fileId);

    FireStoreFile fileToDelete =
        new FireStoreFile()
            .fileId(fileId.toString())
            .bucketResourceId(RESOURCE_ID.toString())
            .gspath("https://differentaccountname.blob.core.windows.net/data/blob.txt");
    assertThat(
        assertThrows(PdaoException.class, () -> dao.deleteFile(fileToDelete, TEST_USER))
            .getMessage(),
        equalTo(
            "Resource groups between metadata storage and request do not match: "
                + "differentaccountname != sa"));
  }

  @Test
  public void testDeleteFileById() {
    UUID fileId = UUID.randomUUID();
    mockFileCopy(fileId);
    when(blobCrl.deleteBlob("data/" + fileId + "/" + SOURCE_FILE_NAME)).thenReturn(true);

    assertThat(
        dao.deleteDataFileById(
            fileId.toString(), SOURCE_FILE_NAME, AZURE_STORAGE_ACCOUNT_RESOURCE, TEST_USER),
        equalTo(true));
  }

  @Test
  public void testDeleteFileByIdNotFound() {
    UUID fileId = UUID.randomUUID();
    mockFileCopy(fileId);
    when(blobCrl.deleteBlob("data/" + fileId + "/" + SOURCE_FILE_NAME)).thenReturn(false);

    assertThat(
        dao.deleteDataFileById(
            fileId.toString(), SOURCE_FILE_NAME, AZURE_STORAGE_ACCOUNT_RESOURCE, TEST_USER),
        equalTo(false));
  }

  @Test
  public void testSasValidation() {
    assertTrue(
        "is valid",
        AzureBlobStorePdao.isSignedUrl(
            BlobUrlParts.parse(
                "https://src.blob.core.windows.net/srcdata/src.txt"
                    + "?sp=r&st=2021-07-14T19:31:16Z&se=2021-07-15T03:31:16Z&spr=https&sv=2020-08-04&"
                    + "sr=b&sig=mysig")));
    assertFalse(
        "no sas token",
        AzureBlobStorePdao.isSignedUrl(
            BlobUrlParts.parse("https://src.blob.core.windows.net/srcdata/src.txt")));
    assertFalse(
        "tld is wrong",
        AzureBlobStorePdao.isSignedUrl(
            BlobUrlParts.parse(
                "https://src.foo.core.windows.net/srcdata/src.txt"
                    + "?sp=r&st=2021-07-14T19:31:16Z&se=2021-07-15T03:31:16Z&spr=https&sv=2020-08-04"
                    + "&sr=b&sig=mysig")));
    assertFalse(
        "missing fields (sr and sig are removed)",
        AzureBlobStorePdao.isSignedUrl(
            BlobUrlParts.parse(
                "https://src.foo.core.windows.net/srcdata/src.txt"
                    + "?sp=r&st=2021-07-14T19:31:16Z&se=2021-07-15T03:31:16Z&spr=https&sv=2020-08-04")));
    assertTrue(
        "extra fields don't hurt",
        AzureBlobStorePdao.isSignedUrl(
            BlobUrlParts.parse(
                "https://src.blob.core.windows.net/srcdata/src.txt"
                    + "?sp=r&st=2021-07-14T19:31:16Z&se=2021-07-15T03:31:16Z&spr=https&sv=2020-08-04&"
                    + "sr=b&sig=mysig")));
  }

  @Test
  public void testValidateUserCanReadSimple() {
    List<String> sourcePaths = List.of("gs://mybucket/myfile.txt");
    dao.validateUserCanRead(sourcePaths, null, TEST_USER);
    verify(gcsPdao, times(1)).validateUserCanRead(sourcePaths, null, TEST_USER, false);
  }

  @Test
  public void testValidateUserCanReadWithUserProject() {
    List<String> sourcePaths =
        List.of(
            "gs://mybucket/myfile1.txt?userProject=foo",
            "gs://mybucket/myfile2.txt?userProject=foo");
    dao.validateUserCanRead(sourcePaths, null, TEST_USER);
    verify(gcsPdao, times(1)).validateUserCanRead(sourcePaths, "foo", TEST_USER, false);
  }

  @Test
  public void testValidateUserCanReadWithMultipleUserProjects() {
    List<String> sourcePaths =
        List.of(
            "gs://mybucket/myfile1.txt?userProject=foo",
            "gs://mybucket/myfile2.txt?userProject=bar");
    TestUtils.assertError(
        IllegalArgumentException.class,
        "Only a single billing project per ingest may be used",
        () -> dao.validateUserCanRead(sourcePaths, null, TEST_USER));
  }

  private FSFileInfo mockFileCopy(UUID fileId) {
    BlobContainerCopySyncPoller poller = mock(BlobContainerCopySyncPoller.class);
    BlobContainerCopier copier = mock(BlobContainerCopier.class);
    BlobProperties blobProperties = mock(BlobProperties.class);
    BlobCopyInfo copyInfo = mock(BlobCopyInfo.class);
    BlobContainerCopyInfo containerCopyInfo =
        new BlobContainerCopyInfo(
            List.of(
                new PollResponse<>(LongRunningOperationStatus.SUCCESSFULLY_COMPLETED, copyInfo)));
    when(blobProperties.getCreationTime()).thenReturn(BLOB_CREATION_TIME);
    when(blobProperties.getBlobSize()).thenReturn(BLOB_SIZE);
    when(blobProperties.getContentMd5()).thenReturn(BLOB_CONTENT_MD5);
    when(copier.beginCopyOperation()).thenReturn(poller);
    when(poller.waitForCompletion())
        .thenReturn(new PollResponse<>(containerCopyInfo.getCopyStatus(), containerCopyInfo));
    when(blobCrl.createBlobContainerCopier(any(), anyString(), anyString())).thenReturn(copier);
    when(blobCrl.createBlobContainerCopier(any(URI.class), anyString())).thenReturn(copier);
    String targetBlobName = "data/" + fileId + "/" + SOURCE_FILE_NAME;
    when(blobCrl.getBlobProperties(targetBlobName)).thenReturn(blobProperties);
    BlobContainerClient sourceBlobContainerClient = mock(BlobContainerClient.class);
    BlobClient blobClient = mock(BlobClient.class);
    // use the same blob properties for the source and for the target
    when(blobClient.getProperties()).thenReturn(blobProperties);
    when(sourceBlobContainerClient.getBlobClient(any())).thenReturn(blobClient);
    when(sourceBlobContainerFactory.getBlobContainerClient()).thenReturn(sourceBlobContainerClient);
    BlobContainerClient targetBlobContainerClient = mock(BlobContainerClient.class);
    when(targetBlobContainerFactory.getBlobContainerClient()).thenReturn(targetBlobContainerClient);
    String targetContainerUrl = "https://" + STORAGE_ACCOUNT_NAME + ".blob.core.windows.net/data";
    when(targetBlobContainerClient.getBlobContainerUrl()).thenReturn(targetContainerUrl);

    // Mock GCS file info
    Blob gcsBlob = mock(Blob.class);
    when(gcsBlob.getMd5ToHexString()).thenReturn(BLOB_CONTENT_MD5_HEX);
    when(gcsBlob.getSize()).thenReturn(BLOB_SIZE);
    when(gcsPdao.getBlobFromGsPathNs(any(), any(), any())).thenReturn(gcsBlob);

    return new FSFileInfo()
        .fileId(fileId.toString())
        .createdDate(BLOB_CREATION_TIME.toInstant().toString())
        .cloudPath(targetContainerUrl + "/" + targetBlobName)
        .checksumMd5(BLOB_CONTENT_MD5_HEX)
        .size(BLOB_SIZE)
        .bucketResourceId(RESOURCE_ID.toString());
  }
}
