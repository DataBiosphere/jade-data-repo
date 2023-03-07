package bio.terra.service.filedata.google.gcs;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.emptyOrNullString;
import static org.hamcrest.Matchers.equalTo;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.when;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.TestUtils;
import bio.terra.common.category.Connected;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.FileLoadModel;
import bio.terra.service.common.gcs.GcsUriUtils;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.filedata.FSFileInfo;
import bio.terra.service.resourcemanagement.google.GoogleBucketResource;
import bio.terra.service.resourcemanagement.google.GoogleProjectResource;
import bio.terra.service.resourcemanagement.google.GoogleResourceDao;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.Storage.ComposeRequest;
import com.google.cloud.storage.StorageOptions;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
@EmbeddedDatabaseTest
public class GcsPdaoTest {

  private static final AuthenticatedUserRequest TEST_USER =
      AuthenticatedUserRequest.builder()
          .setSubjectId("DatasetUnit")
          .setEmail("dataset@unit.com")
          .setToken("token")
          .build();

  @Autowired private ConnectedTestConfiguration testConfig;
  @MockBean private GoogleResourceDao googleResourceDao;
  @Autowired private GcsPdao gcsPdao;

  private final Storage storage = StorageOptions.getDefaultInstance().getService();
  private final String projectId = StorageOptions.getDefaultProjectId();

  @Before
  public void setUp() throws Exception {
    when(googleResourceDao.retrieveProjectByGoogleProjectId(projectId))
        .thenReturn(new GoogleProjectResource().id(UUID.randomUUID()).googleProjectId(projectId));
  }

  @Test
  public void testGetBlobSimple() {
    BlobId testBlob = BlobId.of(testConfig.getIngestbucket(), UUID.randomUUID().toString());

    try {
      storage.create(BlobInfo.newBuilder(testBlob).build());
      Blob blob =
          GcsPdao.getBlobFromGsPath(
              storage, "gs://" + testBlob.getBucket() + "/" + testBlob.getName(), projectId);
      assertNotNull(blob);

      BlobId actualId = blob.getBlobId();
      assertEquals(testBlob.getBucket(), actualId.getBucket());
      assertEquals(testBlob.getName(), actualId.getName());
    } finally {
      storage.delete(testBlob);
    }
  }

  @Test
  public void testGetBlobWithHash() {
    BlobId testBlob =
        BlobId.of(testConfig.getIngestbucket(), UUID.randomUUID() + "#" + UUID.randomUUID());

    try {
      storage.create(BlobInfo.newBuilder(testBlob).build());
      Blob blob =
          GcsPdao.getBlobFromGsPath(
              storage, "gs://" + testBlob.getBucket() + "/" + testBlob.getName(), projectId);
      assertNotNull(blob);

      BlobId actualId = blob.getBlobId();
      assertEquals(testBlob.getBucket(), actualId.getBucket());
      assertEquals(testBlob.getName(), actualId.getName());
    } finally {
      storage.delete(testBlob);
    }
  }

  @Test
  public void testGetBlobContentsMatchingPath() {
    UUID uuid = UUID.randomUUID();
    List<BlobId> blobIds =
        List.of(
            BlobId.of(testConfig.getIngestbucket(), uuid + "/" + uuid + "-1.txt"),
            BlobId.of(testConfig.getIngestbucket(), uuid + "/" + uuid + "-2.txt"),
            BlobId.of(testConfig.getIngestbucket(), uuid + "/" + uuid + "-3.txt"));

    try {
      List<String> contents = new ArrayList<>();
      for (int i = 0; i < blobIds.size(); i++) {
        var fileContents = String.format("This is line %d", i);
        var blobId = blobIds.get(i);
        storage.create(
            BlobInfo.newBuilder(blobId).build(), fileContents.getBytes(StandardCharsets.UTF_8));
        contents.add(fileContents);
      }
      var listPath = GcsUriUtils.getGsPathFromComponents(testConfig.getIngestbucket(), uuid + "/");
      var listLines = getGcsFilesLines(listPath, projectId);
      assertThat(
          "The listed file contents match concatenated contents of individual files",
          listLines,
          equalTo(contents));

      var wildcardMiddlePath =
          GcsUriUtils.getGsPathFromComponents(
              testConfig.getIngestbucket(), uuid + "/" + uuid + "-*.txt");
      var wildcardMiddleLines = getGcsFilesLines(wildcardMiddlePath, projectId);
      assertThat(
          "The middle-wildcard-matched file contents match concatenated contents of individual files",
          wildcardMiddleLines,
          equalTo(contents));

      var wildcardEndPath =
          GcsUriUtils.getGsPathFromComponents(
              testConfig.getIngestbucket(), uuid + "/" + uuid + "-*");
      var wildcardEndLines = getGcsFilesLines(wildcardEndPath, projectId);
      assertThat(
          "The end-wildcard-matched file contents match concatenated contents of individual files",
          wildcardEndLines,
          equalTo(contents));

      var wildcardMultiplePath =
          GcsUriUtils.getGsPathFromComponents(
              testConfig.getIngestbucket(), uuid + "/*" + uuid + "-*.txt");
      var wildcardMutlipleLines = getGcsFilesLines(wildcardMultiplePath, projectId);
      assertThat(
          "Multiple-wildcard paths don't return anything",
          wildcardMutlipleLines,
          emptyCollectionOf(String.class));

    } finally {
      storage.delete(blobIds);
    }
  }

  @Test
  public void testCopyBlobWithPredictableFileId() {
    testCopyBlob(true);
  }

  @Test
  public void testCopyBlobWithoutPredictableFileId() {
    testCopyBlob(false);
  }

  private void testCopyBlob(boolean usePredictableFileId) {
    BlobId sourceBlob =
        BlobId.of(testConfig.getIngestbucket(), UUID.randomUUID() + "#" + UUID.randomUUID());
    String sourcePath = "gs://" + sourceBlob.getBucket() + "/" + sourceBlob.getName();
    FileLoadModel fileLoadModel = new FileLoadModel().sourcePath(sourcePath).targetPath("/foo/bar");
    try {
      String fileId = usePredictableFileId ? null : UUID.randomUUID().toString();
      Dataset dataset =
          new Dataset()
              .id(UUID.randomUUID())
              .predictableFileIds(usePredictableFileId)
              .name("test_dataset");
      // Try to copy using predictable and non-predictable file ids
      gcsPdao.createGcsFile(sourceBlob, projectId);
      gcsPdao.writeStreamToCloudFile(sourcePath, Stream.of("foo", "bar"), projectId);
      // Copy the file once
      GoogleBucketResource targetBucket =
          new GoogleBucketResource()
              .resourceId(UUID.randomUUID())
              .name(sourceBlob.getBucket())
              .projectResource(new GoogleProjectResource().googleProjectId(projectId));
      FSFileInfo fsFileInfo = gcsPdao.copyFile(dataset, fileLoadModel, fileId, targetBucket);
      String initialTime = fsFileInfo.getCreatedDate();
      TimeUnit.SECONDS.sleep(1);
      assertThat(
          "times between copies are the same since the file is the same",
          gcsPdao.copyFile(dataset, fileLoadModel, fileId, targetBucket).getCreatedDate(),
          equalTo(initialTime));
      storage.delete(BlobId.fromGsUtilUri(fsFileInfo.getCloudPath()));
      assertThat(
          "times between copies is different since file was deleted",
          gcsPdao.copyFile(dataset, fileLoadModel, fileId, targetBucket).getCreatedDate(),
          not(equalTo(initialTime)));
    } catch (InterruptedException e) {
      storage.delete(sourceBlob);
    } finally {
      storage.delete(sourceBlob);
    }
  }

  @Test
  public void testCopyBlobWithNoSourceMd5AndPredictableId() {
    testCopyBlobWithNoSourceMd5(true);
  }

  @Test
  public void testCopyBlobWithNoSourceMd5AndRandomId() {
    testCopyBlobWithNoSourceMd5(false);
  }

  public void testCopyBlobWithNoSourceMd5(boolean usePredictableFileId) {
    BlobId sourceBlobId =
        BlobId.of(testConfig.getIngestbucket(), UUID.randomUUID() + "#" + UUID.randomUUID());
    BlobId sourceComposedBlobId =
        BlobId.of(testConfig.getIngestbucket(), UUID.randomUUID() + "#C" + UUID.randomUUID());
    String fileId = usePredictableFileId ? null : UUID.randomUUID().toString();
    try {
      Blob sourceBlob = gcsPdao.createGcsFile(sourceBlobId, projectId);
      gcsPdao.writeStreamToCloudFile(
          sourceBlobId.toGsUtilUri(), Stream.of("foo", "bar"), projectId);
      createCompositeGcsFile(List.of(sourceBlob), sourceComposedBlobId);

      FileLoadModel fileLoadModel =
          new FileLoadModel().sourcePath(sourceComposedBlobId.toGsUtilUri()).targetPath("/foo/bar");

      Dataset dataset =
          new Dataset()
              .id(UUID.randomUUID())
              .predictableFileIds(usePredictableFileId)
              .name("test_dataset");

      GoogleBucketResource targetBucket =
          new GoogleBucketResource()
              .resourceId(UUID.randomUUID())
              .name(sourceBlob.getBucket())
              .projectResource(new GoogleProjectResource().googleProjectId(projectId));
      // Copy the file with no md5 specified (it should fail for predictable ids)
      if (usePredictableFileId) {
        TestUtils.assertError(
            NullPointerException.class,
            "An MD5 checksum is required to create a file id",
            () -> gcsPdao.copyFile(dataset, fileLoadModel, fileId, targetBucket));
      } else {
        FSFileInfo fsFileInfo = gcsPdao.copyFile(dataset, fileLoadModel, fileId, targetBucket);
        assertThat(
            "file created with no md5", fsFileInfo.getChecksumMd5(), is(emptyOrNullString()));
        // Delete the target file
        storage.delete(fsFileInfo.getCloudPath());
      }
      // Copy the file with the md5 specified (it should work).  Note: while this will work, this
      // isn't representative of how this *should* be used.  Typically, the MD5 would be
      // calculated by the user.  This is to demonstrate that the md5 can be anything here (e.g.
      // there is no verification)
      String userMd5 = UUID.randomUUID().toString().replace("-", "");
      fileLoadModel.md5(userMd5);
      FSFileInfo fsFileInfo = gcsPdao.copyFile(dataset, fileLoadModel, fileId, targetBucket);
      assertThat(
          "file created with user specified md5", fsFileInfo.getChecksumMd5(), equalTo(userMd5));
      // Delete the target file
      storage.delete(BlobId.fromGsUtilUri(fsFileInfo.getCloudPath()));
    } finally {
      storage.delete(sourceBlobId, sourceComposedBlobId);
    }
  }

  private List<String> getGcsFilesLines(String path, String projectId) {
    try (var stream = gcsPdao.getBlobsLinesStream(path, projectId, TEST_USER)) {
      return stream.collect(Collectors.toList());
    }
  }

  private void createCompositeGcsFile(Collection<Blob> sourceBlobs, BlobId targetBlobId) {
    storage.compose(
        ComposeRequest.of(
            sourceBlobs.stream().map(b -> b.getBlobId().getName()).toList(),
            BlobInfo.newBuilder(targetBlobId).build()));
  }
}
