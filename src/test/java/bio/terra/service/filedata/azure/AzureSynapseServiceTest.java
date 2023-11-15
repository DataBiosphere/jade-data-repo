package bio.terra.service.filedata.azure;

import bio.terra.common.category.Unit;
import bio.terra.common.fixtures.AuthenticationFixtures;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.service.resourcemanagement.MetadataDataAccessUtils;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@Tag(Unit.TAG)
@ExtendWith(MockitoExtension.class)
public class AzureSynapseServiceTest {
  private AuthenticatedUserRequest testUser = AuthenticationFixtures.randomUserRequest();
  @Mock private AzureSynapsePdao azureSynapsePdao;
  @Mock private MetadataDataAccessUtils metadataDataAccessUtils;
  private AzureSynapseService azureSynapseService;

  @BeforeEach
  public void setUp() {
    azureSynapseService = new AzureSynapseService(azureSynapsePdao, metadataDataAccessUtils);
  }
}
