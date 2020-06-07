package bio.terra.service.filedata.google.gcs;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.category.Connected;
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
    public void testGetBlobWithHash() {
        BlobId testBlob =
            BlobId.of(testConfig.getIngestbucket(), UUID.randomUUID() + "#" + UUID.randomUUID());

        try {
            storage.create(BlobInfo.newBuilder(testBlob).build());
            Blob blob = GcsPdao.getBlobFromGsPath(storage, "gs://" + testBlob.getBucket() + "/" + testBlob.getName());
            Assert.assertNotNull(blob);
            Assert.assertEquals(blob.getBlobId(), testBlob);
        } finally {
            storage.delete(testBlob);
        }
    }
}
