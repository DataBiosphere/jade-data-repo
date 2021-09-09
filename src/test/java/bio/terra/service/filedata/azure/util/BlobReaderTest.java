package bio.terra.service.filedata.azure.util;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.SynapseUtils;
import bio.terra.common.category.Connected;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.blobstore.AzureBlobStorePdao;
import bio.terra.service.filedata.flight.ingest.IngestPopulateFileStateFromFileAzureStep;
import bio.terra.service.load.LoadService;
import com.azure.storage.blob.BlobClient;
import com.azure.storage.blob.BlobClientBuilder;
import com.azure.storage.blob.BlobUrlParts;
import java.io.BufferedReader;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.UUID;
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
public class BlobReaderTest {

  @Autowired SynapseUtils synapseUtils;
  @Autowired private ConnectedTestConfiguration testConfig;
  @Autowired BlobReader blobReader;
  @Autowired AzureBlobStorePdao azureBlobStorePdao;
  @Autowired LoadService loadService;

  @Test
  public void testBlobReader() {
    UUID tenantId = testConfig.getTargetTenantId();
    UUID loadId = UUID.randomUUID();
    String ingestFileLocation =
        synapseUtils.ingestRequestURL(
            testConfig.getSourceStorageAccountName(),
            testConfig.getIngestRequestContainer(),
            "file-ingest-azure.json");

    IngestUtils.validateBlobAzureBlobFileURL(ingestFileLocation);
    BlobUrlParts ingestRequestSignUrlBlob =
        azureBlobStorePdao.getOrSignUrlForSourceFactory(ingestFileLocation, tenantId);
    // This is currently only used in testing - why is that? Am I missing something?
    BlobClient blobClient =
        new BlobClientBuilder().endpoint(ingestRequestSignUrlBlob.toUrl().toString()).buildClient();
    // ByteArrayOutputStream stream = new ByteArrayOutputStream();
    // blobClient.downloadStream(stream);
    InputStream inputStream = blobClient.openInputStream();
    IngestPopulateFileStateFromFileAzureStep step =
        new IngestPopulateFileStateFromFileAzureStep(loadService, 10, 10, azureBlobStorePdao);
    step.readFile(new BufferedReader(new InputStreamReader(inputStream)), loadId);
  }
}
