package bio.terra.datarepo.service.filedata.google.gcs;

import bio.terra.datarepo.app.configuration.ConnectedTestConfiguration;
import bio.terra.datarepo.common.category.Connected;
import bio.terra.datarepo.common.exception.PdaoInvalidUriException;
import bio.terra.datarepo.common.exception.PdaoSourceFileNotFoundException;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import java.util.UUID;
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
}
