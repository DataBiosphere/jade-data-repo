package bio.terra.service.filedata.azure.blobstore;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
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
import bio.terra.service.filedata.google.gcs.GcsProjectFactory;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.azure.AzureAuthService;
import bio.terra.service.resourcemanagement.azure.AzureContainerPdao;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import bio.terra.service.resourcemanagement.azure.AzureResourceDao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import com.azure.core.credential.TokenCredential;
import com.azure.core.http.rest.PagedIterable;
import com.azure.core.util.polling.LongRunningOperationStatus;
import com.azure.core.util.polling.PollResponse;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.BlobUrlParts;
import com.azure.storage.blob.models.BlobCopyInfo;
import com.azure.storage.blob.models.BlobItem;
import com.azure.storage.blob.models.BlobProperties;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.BlobGetOption;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.core.task.AsyncTaskExecutor;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.bean.override.mockito.MockitoBean;
import org.springframework.test.context.bean.override.mockito.MockitoSpyBean;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Tag(Unit.TAG)
@EmbeddedDatabaseTest
class AzureBlobStorePdaoTest {
  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticatedUserRequest.builder()
          .setSubjectId("DatasetUnit")
          .setEmail("dataset@unit.com")
          .setToken("token")
          .build();
  private static final UUID PROFILE_ID = UUID.randomUUID();
  private static final UUID RESOURCE_ID = UUID.randomUUID();
  private static final UUID TENANT_ID = UUID.randomUUID();
  private static final UUID DATASET_ID = UUID.randomUUID();
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
  @MockitoBean private ProfileDao profileDao;
  @MockitoBean private AzureContainerPdao azureContainerPdao;
  @MockitoBean private AzureResourceConfiguration resourceConfiguration;
  @MockitoBean private AzureResourceDao azureResourceDao;
  @MockitoBean private AzureAuthService azureAuthService;
  @MockitoBean private GcsPdao gcsPdao;
  @MockitoBean private GcsProjectFactory gcsProjectFactory;
  @MockitoBean private AzureBlobService azureBlobService;

  @MockitoBean(name = AzureResourceConfiguration.TABLE_THREADPOOL_NAME)
  private AsyncTaskExecutor asyncTaskExecutor;

  @MockitoSpyBean private AzureBlobStorePdao dao;

  @MockitoBean
  @Qualifier("synapseJdbcTemplate")
  private NamedParameterJdbcTemplate synapseJdbcTemplate;

  private FileLoadModel fileLoadModel;
  private Dataset dataset;

  @BeforeEach
  void setUp() {
    TokenCredential targetCredential = mock(TokenCredential.class);
    fileLoadModel =
        new FileLoadModel()
            .profileId(PROFILE_ID)
            .targetPath(TARGET_PATH)
            .loadTag(LOAD_TAG)
            .mimeType(MIME_TYPE);
    dataset = new Dataset().id(DATASET_ID);
    when(profileDao.getBillingProfileById(PROFILE_ID)).thenReturn(BILLING_PROFILE);
    when(resourceConfiguration.getAppToken(TENANT_ID)).thenReturn(targetCredential);
    when(resourceConfiguration.maxRetries()).thenReturn(3);
    when(resourceConfiguration.retryTimeoutSeconds()).thenReturn(60);
    when(azureResourceDao.retrieveStorageAccountById(RESOURCE_ID))
        .thenReturn(AZURE_STORAGE_ACCOUNT_RESOURCE);
    targetBlobContainerFactory = mock(BlobContainerClientFactory.class);
    sourceBlobContainerFactory = mock(BlobContainerClientFactory.class);
    blobCrl = mock(BlobCrl.class);
    doReturn(targetBlobContainerFactory).when(dao).getTargetDataClientFactory(any(), any(), any());
    when(azureBlobService.getSourceClientFactory(anyString(), any(), anyString()))
        .thenReturn(sourceBlobContainerFactory);
    when(azureBlobService.getSourceClientFactory(any())).thenReturn(sourceBlobContainerFactory);
    when(azureBlobService.getBlobCrl(any())).thenReturn(blobCrl);
  }

  @Test
  void testCopyFile() {
    UUID fileId = UUID.randomUUID();
    fileLoadModel.sourcePath(SOURCE_PATH);

    FSFileInfo expectedFileInfo = mockFileCopy(fileId);

    FSFileInfo fsFileInfo =
        dao.copyFile(
            dataset.predictableFileIds(false),
            BILLING_PROFILE,
            fileLoadModel,
            fileId.toString(),
            AZURE_STORAGE_ACCOUNT_RESOURCE,
            TEST_USER);
    assertThat("output is expected", fsFileInfo, samePropertyValuesAs(expectedFileInfo));
  }

  @Test
  void testCopyFileWithSas() {
    UUID fileId = UUID.randomUUID();
    fileLoadModel.sourcePath(
        SOURCE_PATH
            + "?sp=r&st=2021-07-14T19:31:16Z&se=2021-07-15T03:31:16Z&"
            + "spr=https&sv=2020-08-04&sr=b&sig=mysig");

    FSFileInfo expectedFileInfo = mockFileCopy(fileId);

    FSFileInfo fsFileInfo =
        dao.copyFile(
            dataset.predictableFileIds(false),
            BILLING_PROFILE,
            fileLoadModel,
            fileId.toString(),
            AZURE_STORAGE_ACCOUNT_RESOURCE,
            TEST_USER);
    assertThat("output is expected", fsFileInfo, samePropertyValuesAs(expectedFileInfo));
  }

  @Test
  void testCopyFileWithGcsFile() {
    UUID fileId = UUID.randomUUID();
    fileLoadModel.sourcePath(SOURCE_GCS_PATH);

    mockGcsFileAccess(null);
    FSFileInfo expectedFileInfo = mockFileCopy(fileId);

    FSFileInfo fsFileInfo =
        dao.copyFile(
            dataset.predictableFileIds(false),
            BILLING_PROFILE,
            fileLoadModel,
            fileId.toString(),
            AZURE_STORAGE_ACCOUNT_RESOURCE,
            TEST_USER);
    assertThat("output is expected", fsFileInfo, samePropertyValuesAs(expectedFileInfo));
  }

  @Test
  void testCopyFileWithGcsFileAndProject() {
    UUID fileId = UUID.randomUUID();
    String userProject = "foo";
    String sourcePath = SOURCE_GCS_PATH + "?userProject=" + userProject;
    fileLoadModel.sourcePath(sourcePath);

    mockGcsFileAccess(userProject);
    FSFileInfo expectedFileInfo = mockFileCopy(fileId);
    FSFileInfo fsFileInfo =
        dao.copyFile(
            dataset.predictableFileIds(false),
            BILLING_PROFILE,
            fileLoadModel,
            fileId.toString(),
            AZURE_STORAGE_ACCOUNT_RESOURCE,
            TEST_USER);
    assertThat("output is expected", fsFileInfo, samePropertyValuesAs(expectedFileInfo));
  }

  @Test
  void testDeleteFile() {
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
  void testDeleteFileNotFound() {
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
  void testDeleteFileMismatchedStorageAccount() {
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
  void testDeleteFileById() {
    UUID fileId = UUID.randomUUID();
    mockFileCopy(fileId);
    when(blobCrl.deleteBlob("data/" + fileId + "/" + SOURCE_FILE_NAME)).thenReturn(true);

    assertThat(
        dao.deleteDataFileById(
            fileId.toString(), SOURCE_FILE_NAME, AZURE_STORAGE_ACCOUNT_RESOURCE, TEST_USER),
        equalTo(true));
  }

  @Test
  void testDeleteFileByIdNotFound() {
    UUID fileId = UUID.randomUUID();
    mockFileCopy(fileId);
    when(blobCrl.deleteBlob("data/" + fileId + "/" + SOURCE_FILE_NAME)).thenReturn(false);

    assertThat(
        dao.deleteDataFileById(
            fileId.toString(), SOURCE_FILE_NAME, AZURE_STORAGE_ACCOUNT_RESOURCE, TEST_USER),
        equalTo(false));
  }

  @Test
  void testSasValidation() {
    assertThat(
        "is valid",
        AzureBlobStorePdao.isSignedUrl(
            BlobUrlParts.parse(
                "https://src.blob.core.windows.net/srcdata/src.txt"
                    + "?sp=r&st=2021-07-14T19:31:16Z&se=2021-07-15T03:31:16Z&spr=https&sv=2020-08-04&"
                    + "sr=b&sig=mysig")));
    assertThat(
        "no sas token",
        not(
            AzureBlobStorePdao.isSignedUrl(
                BlobUrlParts.parse("https://src.blob.core.windows.net/srcdata/src.txt"))));
    assertThat(
        "tld is wrong",
        not(
            AzureBlobStorePdao.isSignedUrl(
                BlobUrlParts.parse(
                    "https://src.foo.core.windows.net/srcdata/src.txt"
                        + "?sp=r&st=2021-07-14T19:31:16Z&se=2021-07-15T03:31:16Z&spr=https&sv=2020-08-04"
                        + "&sr=b&sig=mysig"))));
    assertThat(
        "missing fields (sr and sig are removed)",
        not(
            AzureBlobStorePdao.isSignedUrl(
                BlobUrlParts.parse(
                    "https://src.foo.core.windows.net/srcdata/src.txt"
                        + "?sp=r&st=2021-07-14T19:31:16Z&se=2021-07-15T03:31:16Z&spr=https&sv=2020-08-04"))));
    assertThat(
        "extra fields don't hurt",
        AzureBlobStorePdao.isSignedUrl(
            BlobUrlParts.parse(
                "https://src.blob.core.windows.net/srcdata/src.txt"
                    + "?sp=r&st=2021-07-14T19:31:16Z&se=2021-07-15T03:31:16Z&spr=https&sv=2020-08-04&"
                    + "sr=b&sig=mysig")));
  }

  @Test
  void testValidateUserCanReadSimple() {
    List<String> sourcePaths = List.of("gs://mybucket/myfile.txt");
    dao.validateUserCanRead(sourcePaths, null, TEST_USER, dataset);
    verify(gcsPdao).validateUserCanRead(sourcePaths, null, TEST_USER, dataset);
  }

  @Test
  void testValidateUserCanReadWithUserProject() {
    List<String> sourcePaths =
        List.of(
            "gs://mybucket/myfile1.txt?userProject=foo",
            "gs://mybucket/myfile2.txt?userProject=foo");
    dao.validateUserCanRead(sourcePaths, null, TEST_USER, dataset);
    verify(gcsPdao).validateUserCanRead(sourcePaths, "foo", TEST_USER, dataset);
  }

  @Test
  void testValidateUserCanReadWithMultipleUserProjects() {
    List<String> sourcePaths =
        List.of(
            "gs://mybucket/myfile1.txt?userProject=foo",
            "gs://mybucket/myfile2.txt?userProject=bar");
    TestUtils.assertError(
        IllegalArgumentException.class,
        "Only a single billing project per ingest may be used",
        () -> dao.validateUserCanRead(sourcePaths, null, TEST_USER, dataset));
  }

  @Test
  void testListChildren() {
    String baseBlobName = "metadata/parquet/my_table.parquet";
    String url = "https://src.blob.core.windows.net/snapid/%s".formatted(baseBlobName);
    String sasToken = "sp=r";
    List<String> blobNames = List.of("/_", "/parquetchunk.part");
    List<BlobItem> blobItems =
        blobNames.stream()
            .map(
                p -> {
                  BlobItem blob = mock(BlobItem.class);
                  when(blob.getName()).thenReturn(baseBlobName + p);
                  return blob;
                })
            .toList();
    PagedIterable<BlobItem> blobItemsStream = mock(PagedIterable.class);
    when(blobItemsStream.stream()).thenReturn(blobItems.stream());
    BlobContainerClient sourceBlobContainerClient = mock(BlobContainerClient.class);
    when(sourceBlobContainerFactory.getBlobContainerClient()).thenReturn(sourceBlobContainerClient);
    when(sourceBlobContainerClient.listBlobs(any(), any())).thenReturn(blobItemsStream);
    assertThat(
        "Returned url is properly encoded and filtered",
        dao.listChildren("%s?%s".formatted(url, sasToken)).stream().toList(),
        contains("%s%s?%s".formatted(url, "/parquetchunk.part", sasToken)));
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

    return new FSFileInfo()
        .fileId(fileId.toString())
        .createdDate(BLOB_CREATION_TIME.toInstant().toString())
        .cloudPath(targetContainerUrl + "/" + targetBlobName)
        .checksumMd5(BLOB_CONTENT_MD5_HEX)
        .size(BLOB_SIZE)
        .bucketResourceId(RESOURCE_ID.toString());
  }

  private void mockGcsFileAccess(String projectId) {
    Storage storage = mock(Storage.class);
    Blob blob = mock(Blob.class);
    when(storage.get(eq(BlobId.fromGsUtilUri(SOURCE_GCS_PATH)), any(BlobGetOption[].class)))
        .thenReturn(blob);
    when(gcsProjectFactory.getStorage(projectId)).thenReturn(storage);
  }
}
