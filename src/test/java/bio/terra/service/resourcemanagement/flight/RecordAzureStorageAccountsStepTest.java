package bio.terra.service.resourcemanagement.flight;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasSize;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountService;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class RecordAzureStorageAccountsStepTest {
  @Mock private AzureStorageAccountService azureStorageAccountService;
  private RecordAzureStorageAccountsStep recordAzureStorageAccountsStep;

  @BeforeEach
  void setUp() {
    recordAzureStorageAccountsStep = new RecordAzureStorageAccountsStep(azureStorageAccountService);
  }

  @Test
  void getUniqueStorageAccounts() {
    List<UUID> appIdList = List.of(UUID.randomUUID());
    AzureStorageAccountResource storageAccount1 =
        new AzureStorageAccountResource().name("account1").topLevelContainer("container1");
    AzureStorageAccountResource storageAccount2 =
        new AzureStorageAccountResource().name("account1").topLevelContainer("container2");
    when(azureStorageAccountService.listStorageAccountPerAppDeployment(appIdList, true))
        .thenReturn(List.of(storageAccount1, storageAccount2));
    List<AzureStorageAccountResource> resources =
        recordAzureStorageAccountsStep.getUniqueStorageAccounts(appIdList);
    assertThat("Only one unique storage account should be returned", resources, hasSize(1));
  }
}
