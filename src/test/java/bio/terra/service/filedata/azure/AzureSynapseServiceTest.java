package bio.terra.service.filedata.azure;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

import bio.terra.common.EmbeddedDatabaseTest;
import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.AccessInfoModel;
import bio.terra.model.AccessInfoParquetModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.resourcemanagement.MetadataDataAccessUtils;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.ActiveProfiles;

@SpringBootTest
@AutoConfigureMockMvc
@ActiveProfiles({"google", "unittest"})
@Tag(Unit.TAG)
@EmbeddedDatabaseTest
public class AzureSynapseServiceTest {
  private AuthenticatedUserRequest testUser = AuthenticationFixtures.randomUserRequest();
  @Mock private AzureSynapsePdao azureSynapsePdao;
  @Mock private MetadataDataAccessUtils metadataDataAccessUtils;
  private AzureSynapseService azureSynapseService;

  @BeforeEach
  public void setUp() {
    azureSynapseService = new AzureSynapseService(azureSynapsePdao, metadataDataAccessUtils);
  }

  @Test
  public void getOrCreateExternalAzureDataSource() throws Exception {
    UUID datasetId = UUID.randomUUID();
    Dataset dataset = new Dataset().id(datasetId);
    when(metadataDataAccessUtils.accessInfoFromDataset(dataset, testUser))
        .thenReturn(
            new AccessInfoModel()
                .parquet(new AccessInfoParquetModel().sasToken("sas_token").url("url")));
    doNothing()
        .when(azureSynapsePdao)
        .getOrCreateExternalDataSource(eq("url?sas_token"), any(), any());

    azureSynapseService.getOrCreateExternalAzureDataSource(dataset, testUser);
  }
}
