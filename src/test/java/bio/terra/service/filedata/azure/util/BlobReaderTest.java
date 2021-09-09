package bio.terra.service.filedata.azure.util;

import bio.terra.app.configuration.ConnectedTestConfiguration;
import bio.terra.common.SynapseUtils;
import bio.terra.common.category.Connected;
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

  @Test
  public void testBlobReader() {
    UUID tenantId = testConfig.getTargetTenantId();
    String ingestFileLocation =
        synapseUtils.ingestRequestURL(
            testConfig.getSourceStorageAccountName(),
            testConfig.getIngestRequestContainer(),
            "file-ingest-azure.json");

    blobReader.readFromBlob(tenantId, ingestFileLocation, 10, UUID.randomUUID());
  }
}
