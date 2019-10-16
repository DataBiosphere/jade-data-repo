package bio.terra.flight.dataset.ingest;

import bio.terra.category.Connected;
import bio.terra.configuration.ConnectedTestConfiguration;
import bio.terra.flight.exception.IngestFileNotFoundException;
import bio.terra.flight.exception.InvalidUriException;
import com.google.cloud.storage.Blob;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.Storage;
import com.google.cloud.storage.StorageOptions;
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
public class IngestSetupStepTest {

    @Autowired
    ConnectedTestConfiguration testConfig;

    @Test
    public void testValidSingleSourceFile() {
        Storage storageClient = StorageOptions.getDefaultInstance().getService();
        BlobId testBlobId = BlobId.of(testConfig.getIngestbucket(), "some/file.json");
        Blob testBlob = null;

        try {
            testBlob = storageClient.create(BlobInfo.newBuilder(testBlobId).build());
            IngestSetupStep.validateSourceUri("gs://" + testBlobId.getBucket() + "/" + testBlobId.getName());
        } finally {
            if (testBlob != null) {
                storageClient.delete(testBlobId);
            }
        }
    }

    @Test
    public void testValidSourcePattern() {
        IngestSetupStep.validateSourceUri("gs://some-bucket/some/prefix*");
        IngestSetupStep.validateSourceUri("gs://some-bucket/some*pattern");
    }

    @Test(expected = InvalidUriException.class)
    public void testNotAGsUri() {
        IngestSetupStep.validateSourceUri("https://foo.com/bar");
    }

    @Test(expected = InvalidUriException.class)
    public void testInvalidBucketWildcard() {
        IngestSetupStep.validateSourceUri("gs://some-bucket-*/some/file/path");
    }

    @Test(expected = IngestFileNotFoundException.class)
    public void testMissingSingleSourceFile() {
        IngestSetupStep.validateSourceUri("gs://some-bucket/some/file.json");
    }

    @Test(expected = InvalidUriException.class)
    public void testInvalidMultiWildcard() {
        IngestSetupStep.validateSourceUri("gs://some-bucket/some/prefix*/some*pattern");
    }
}
