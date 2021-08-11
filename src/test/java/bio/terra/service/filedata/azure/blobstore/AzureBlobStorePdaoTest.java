package bio.terra.service.filedata.azure.blobstore;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.samePropertyValuesAs;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.exception.PdaoException;
import bio.terra.model.BillingProfileModel;
import bio.terra.model.FileLoadModel;
import bio.terra.service.filedata.FSFileInfo;
import bio.terra.service.filedata.azure.util.BlobContainerClientFactory;
import bio.terra.service.filedata.azure.util.BlobContainerCopier;
import bio.terra.service.filedata.azure.util.BlobContainerCopySyncPoller;
import bio.terra.service.filedata.azure.util.BlobCrl;
import bio.terra.service.filedata.google.firestore.FireStoreFile;
import bio.terra.service.profile.ProfileDao;
import bio.terra.service.resourcemanagement.azure.AzureContainerPdao;
import bio.terra.service.resourcemanagement.azure.AzureResourceConfiguration;
import bio.terra.service.resourcemanagement.azure.AzureResourceDao;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import com.azure.core.credential.TokenCredential;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobProperties;
import com.azure.storage.blob.models.BlobStorageException;
import java.nio.charset.StandardCharsets;
import java.time.OffsetDateTime;
import java.util.UUID;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.http.HttpStatus;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class AzureBlobStorePdaoTest {
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
  private static final String TARGET_PATH = "/foo/bar.txt";
  private static final OffsetDateTime BLOB_CREATION_TIME = OffsetDateTime.now();
  private static final long BLOB_SIZE = 1234567890L;
  private static final byte[] BLOB_CONTENT_MD5 = "FOOBAR".getBytes(StandardCharsets.UTF_8);
  // This is the base64 encoding of FOOBAR
  private static final String BLOB_CONTENT_MD5_B64 = "Rk9PQkFS";
  private static final String LOAD_TAG = "tag";
  private static final String MIME_TYPE = "txt/plain";

  private BlobContainerClientFactory sourceBlobContainerFactory;
  private BlobContainerClientFactory targetBlobContainerFactory;
  private BlobCrl blobCrl;
  @MockBean private ProfileDao profileDao;
  @MockBean private AzureContainerPdao azureContainerPdao;
  @MockBean private AzureResourceConfiguration resourceConfiguration;
  @MockBean private AzureResourceDao azureResourceDao;
  @Autowired private AzureBlobStorePdao dao;

  private FileLoadModel fileLoadModel;

  @Before
  public void setUp() throws Exception {
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
    when(azureResourceDao.retrieveStorageAccountById(RESOURCE_ID))
        .thenReturn(AZURE_STORAGE_ACCOUNT_RESOURCE);
    targetBlobContainerFactory = mock(BlobContainerClientFactory.class);
    sourceBlobContainerFactory = mock(BlobContainerClientFactory.class);
    blobCrl = mock(BlobCrl.class);
    doReturn(targetBlobContainerFactory)
        .when(dao)
        .getTargetDataClientFactory(any(), any(), anyBoolean());
    doReturn(sourceBlobContainerFactory)
        .when(dao)
        .getSourceClientFactory(anyString(), any(), anyString());
    doReturn(sourceBlobContainerFactory).when(dao).getSourceClientFactory(anyString());
    doReturn(blobCrl).when(dao).getBlobCrl(any());
  }

  @Test
  public void testCopyFile() {
    UUID fileId = UUID.randomUUID();
    fileLoadModel.sourcePath(SOURCE_PATH);

    FSFileInfo expectedFileInfo = mockFileCopy(fileId);

    FSFileInfo fsFileInfo =
        dao.copyFile(fileLoadModel, fileId.toString(), AZURE_STORAGE_ACCOUNT_RESOURCE);
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
        dao.copyFile(fileLoadModel, fileId.toString(), AZURE_STORAGE_ACCOUNT_RESOURCE);
    assertThat("output is expected", fsFileInfo, samePropertyValuesAs(expectedFileInfo));
  }

  @Test
  public void testDeleteFile() {
    UUID fileId = UUID.randomUUID();
    FSFileInfo fsFileInfo = mockFileCopy(fileId);
    doNothing().when(blobCrl).deleteBlob(fileId + "/" + SOURCE_FILE_NAME);

    FireStoreFile fileToDelete =
        new FireStoreFile()
            .fileId(fileId.toString())
            .bucketResourceId(RESOURCE_ID.toString())
            .gspath(fsFileInfo.getCloudPath());
    assertThat(dao.deleteFile(fileToDelete), equalTo(true));
  }

  @Test
  public void testDeleteFileNotFound() {
    UUID fileId = UUID.randomUUID();
    FSFileInfo fsFileInfo = mockFileCopy(fileId);
    BlobStorageException exception = mock(BlobStorageException.class);
    when(exception.getStatusCode()).thenReturn(HttpStatus.NOT_FOUND.value());
    doThrow(exception).when(blobCrl).deleteBlob(fileId + "/" + SOURCE_FILE_NAME);

    FireStoreFile fileToDelete =
        new FireStoreFile()
            .fileId(fileId.toString())
            .bucketResourceId(RESOURCE_ID.toString())
            .gspath(fsFileInfo.getCloudPath());
    assertThat(dao.deleteFile(fileToDelete), equalTo(false));
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
        assertThrows(PdaoException.class, () -> dao.deleteFile(fileToDelete)).getMessage(),
        equalTo(
            "Resource groups between metadata storage and request do not match: "
                + "differentaccountname != sa"));
  }

  @Test
  public void testDeleteFileById() {
    UUID fileId = UUID.randomUUID();
    mockFileCopy(fileId);
    doNothing().when(blobCrl).deleteBlob(fileId + "/" + SOURCE_FILE_NAME);

    assertThat(
        dao.deleteDataFileById(fileId.toString(), SOURCE_FILE_NAME, AZURE_STORAGE_ACCOUNT_RESOURCE),
        equalTo(true));
  }

  @Test
  public void testDeleteFileByIdNotFound() {
    UUID fileId = UUID.randomUUID();
    mockFileCopy(fileId);
    BlobStorageException exception = mock(BlobStorageException.class);
    when(exception.getStatusCode()).thenReturn(HttpStatus.NOT_FOUND.value());
    doThrow(exception).when(blobCrl).deleteBlob(fileId + "/" + SOURCE_FILE_NAME);

    assertThat(
        dao.deleteDataFileById(fileId.toString(), SOURCE_FILE_NAME, AZURE_STORAGE_ACCOUNT_RESOURCE),
        equalTo(false));
  }

  @Test
  public void testSasValidation() {
    assertTrue(
        "is valid",
        AzureBlobStorePdao.isSignedUrl(
            "https://src.blob.core.windows.net/srcdata/src.txt"
                + "?sp=r&st=2021-07-14T19:31:16Z&se=2021-07-15T03:31:16Z&spr=https&sv=2020-08-04&"
                + "sr=b&sig=mysig"));
    assertFalse(
        "no sas token",
        AzureBlobStorePdao.isSignedUrl("https://src.blob.core.windows.net/srcdata/src.txt"));
    assertFalse(
        "tld is wrong",
        AzureBlobStorePdao.isSignedUrl(
            "https://src.foo.core.windows.net/srcdata/src.txt"
                + "?sp=r&st=2021-07-14T19:31:16Z&se=2021-07-15T03:31:16Z&spr=https&sv=2020-08-04"
                + "&sr=b&sig=mysig"));
    assertFalse(
        "missing fields (sr and sig are removed)",
        AzureBlobStorePdao.isSignedUrl(
            "https://src.foo.core.windows.net/srcdata/src.txt"
                + "?sp=r&st=2021-07-14T19:31:16Z&se=2021-07-15T03:31:16Z&spr=https&sv=2020-08-04"));
    assertTrue(
        "extra fields don't hurt",
        AzureBlobStorePdao.isSignedUrl(
            "https://src.blob.core.windows.net/srcdata/src.txt"
                + "?sp=r&st=2021-07-14T19:31:16Z&se=2021-07-15T03:31:16Z&spr=https&sv=2020-08-04&"
                + "sr=b&sig=mysig"));
  }

  private FSFileInfo mockFileCopy(UUID fileId) {
    BlobContainerCopySyncPoller poller = mock(BlobContainerCopySyncPoller.class);
    BlobContainerCopier copier = mock(BlobContainerCopier.class);
    BlobProperties blobProperties = mock(BlobProperties.class);
    when(blobProperties.getCreationTime()).thenReturn(BLOB_CREATION_TIME);
    when(blobProperties.getBlobSize()).thenReturn(BLOB_SIZE);
    when(blobProperties.getContentMd5()).thenReturn(BLOB_CONTENT_MD5);
    when(copier.beginCopyOperation()).thenReturn(poller);
    when(blobCrl.createBlobContainerCopier(any(), anyString(), anyString())).thenReturn(copier);
    String targetBlobName = fileId + "/" + SOURCE_FILE_NAME;
    when(blobCrl.getBlobProperties(targetBlobName)).thenReturn(blobProperties);
    BlobContainerClient targetBlobContainerClient = mock(BlobContainerClient.class);
    when(targetBlobContainerFactory.getBlobContainerClient()).thenReturn(targetBlobContainerClient);
    String targetContainerUrl = "https://" + STORAGE_ACCOUNT_NAME + ".blob.core.windows.net/data";
    when(targetBlobContainerClient.getBlobContainerUrl()).thenReturn(targetContainerUrl);

    return new FSFileInfo()
        .fileId(fileId.toString())
        .createdDate(BLOB_CREATION_TIME.toInstant().toString())
        .cloudPath(targetContainerUrl + "/" + targetBlobName)
        .checksumMd5(BLOB_CONTENT_MD5_B64)
        .size(BLOB_SIZE)
        .bucketResourceId(RESOURCE_ID.toString());
  }
}
