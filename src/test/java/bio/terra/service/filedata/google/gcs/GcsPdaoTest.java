package bio.terra.service.filedata.google.gcs;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.category.Connected;
import bio.terra.common.exception.PdaoInvalidUriException;
import bio.terra.common.exception.PdaoSourceFileNotFoundException;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.UUID;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "connectedtest"})
@Category(Connected.class)
public class GcsPdaoTest {
    @Autowired private ConnectedTestConfiguration testConfig;

    private Storage storage = StorageOptions.getDefaultInstance().getService();

    @Test
    public void testGetBlobSimple() {
        BlobId testBlob = BlobId.of(testConfig.getIngestbucket(), UUID.randomUUID().toString());

        try {
            storage.create(BlobInfo.newBuilder(testBlob).build());
            Blob blob = GcsPdao.getBlobFromGsPath(storage, "gs://" + testBlob.getBucket() + "/" + testBlob.getName());
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
            Blob blob = GcsPdao.getBlobFromGsPath(storage, "gs://" + testBlob.getBucket() + "/" + testBlob.getName());
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
        GcsPdao.getBlobFromGsPath(storage, "s3://my-aws-bucket/my-cool-path");
    }

    @Test(expected = PdaoInvalidUriException.class)
    public void testGetBlobBucketNameTooShort() {
        GcsPdao.getBlobFromGsPath(storage, "gs://ab/some-path");
    }

    @Test(expected = PdaoInvalidUriException.class)
    public void testGetBlobNoObjectName() {
        GcsPdao.getBlobFromGsPath(storage, "gs://bucket");
    }

    @Test(expected = PdaoSourceFileNotFoundException.class)
    public void testGetBlobNonexistent() {
        GcsPdao.getBlobFromGsPath(storage, "gs://" + testConfig.getIngestbucket() + "/file-doesnt-exist");
    }
}
