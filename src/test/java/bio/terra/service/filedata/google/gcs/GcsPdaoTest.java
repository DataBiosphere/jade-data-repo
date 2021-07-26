package bio.terra.service.filedata.google.gcs;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.emptyCollectionOf;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.category.Connected;
import bio.terra.common.exception.PdaoInvalidUriException;
import bio.terra.common.exception.PdaoSourceFileNotFoundException;
import bio.terra.service.common.gcs.GcsUriUtils;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class GcsPdaoTest {
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired private GcsPdao gcsPdao;

  private Storage storage = StorageOptions.getDefaultInstance().getService();
  private String projectId = StorageOptions.getDefaultProjectId();

  @Test
  public void testGetBlobSimple() {
    BlobId testBlob = BlobId.of(testConfig.getIngestbucket(), UUID.randomUUID().toString());

    try {
      storage.create(BlobInfo.newBuilder(testBlob).build());
      Blob blob =
          GcsPdao.getBlobFromGsPath(
              storage, "gs://" + testBlob.getBucket() + "/" + testBlob.getName(), projectId);
      Assert.assertNotNull(blob);

      BlobId actualId = blob.getBlobId();
      Assert.assertEquals(testBlob.getBucket(), actualId.getBucket());
      Assert.assertEquals(testBlob.getName(), actualId.getName());
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
      Assert.assertNotNull(blob);

      BlobId actualId = blob.getBlobId();
      Assert.assertEquals(testBlob.getBucket(), actualId.getBucket());
      Assert.assertEquals(testBlob.getName(), actualId.getName());
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
          GcsUriUtils.getGsPathFromComponents(testConfig.getIngestbucket(), uuid + "/" + uuid + "-*");
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

  @Test(expected = PdaoInvalidUriException.class)
  public void testGetBlobNonGs() {
    GcsPdao.getBlobFromGsPath(storage, "s3://my-aws-bucket/my-cool-path", projectId);
  }

  @Test(expected = PdaoInvalidUriException.class)
  public void testGetBlobBucketNameTooShort() {
    GcsPdao.getBlobFromGsPath(storage, "gs://ab/some-path", projectId);
  }

  @Test(expected = PdaoInvalidUriException.class)
  public void testGetBlobBucketNameTooLong() {
    StringBuilder bucket = new StringBuilder();
    for (int i = 0; i < 222; i++) {
      if (i != 0) bucket.append(".");
      bucket.append("component");
    }
    GcsPdao.getBlobFromGsPath(storage, "gs://" + bucket.toString() + "/some-path", projectId);
  }

  @Test(expected = PdaoInvalidUriException.class)
  public void testGetBlobBucketNameComponentTooLong() {
    StringBuilder bucket = new StringBuilder();
    for (int i = 0; i < 64; i++) {
      bucket.append("a");
    }
    GcsPdao.getBlobFromGsPath(storage, "gs://" + bucket.toString() + "/some-path", projectId);
  }

  @Test(expected = PdaoInvalidUriException.class)
  public void testGetBlobBucketInvalidCharacters() {
    GcsPdao.getBlobFromGsPath(storage, "gs://AFSDAFADSFADSFASF@@@@/foo", projectId);
  }

  @Test(expected = PdaoInvalidUriException.class)
  public void testGetBlobNoObjectName() {
    GcsPdao.getBlobFromGsPath(storage, "gs://bucket", projectId);
  }

  @Test(expected = PdaoSourceFileNotFoundException.class)
  public void testGetBlobNonexistent() {
    GcsPdao.getBlobFromGsPath(
        storage, "gs://" + testConfig.getIngestbucket() + "/file-doesnt-exist", projectId);
  }

  private List<String> getGcsFilesLines(String path, String projectId) {
    try (var stream = gcsPdao.getGcsFilesLinesStream(path, projectId)) {
      return stream.collect(Collectors.toList());
    }
  }
}
