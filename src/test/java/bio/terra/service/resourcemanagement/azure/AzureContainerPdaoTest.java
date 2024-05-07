package bio.terra.service.resourcemanagement.azure;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.model.BillingProfileModel;
import com.azure.storage.blob.BlobContainerClient;
import com.azure.storage.blob.models.BlobContainerProperties;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class AzureContainerPdaoTest {

  @Mock private AzureAuthService authService;
  @Mock private BlobContainerClient blobContainerClient;
  private AzureStorageAccountResource storageAccountResource;
  private BillingProfileModel billingProfile;
  private AzureContainerPdao dao;

  @BeforeEach
  void setUp() {

    billingProfile = new BillingProfileModel();
    storageAccountResource =
        new AzureStorageAccountResource()
            .name("mystorageaccount")
            .metadataContainer("md")
            .dataContainer("d")
            .topLevelContainer("tld");
    when(authService.getBlobContainerClient(any(), any(), eq("tld")))
        .thenReturn(blobContainerClient);
    dao = new AzureContainerPdao(authService);
  }

  @Test
  void testGetContainer() {
    BlobContainerProperties properties = mock(BlobContainerProperties.class);
    when(blobContainerClient.exists()).thenReturn(true);
    when(properties.getETag()).thenReturn("TAG");
    when(blobContainerClient.getProperties()).thenReturn(properties);

    assertThat(
        "same object is returned",
        dao.getOrCreateContainer(billingProfile, storageAccountResource).getProperties().getETag(),
        equalTo("TAG"));

    verify(blobContainerClient, times(0)).create();
  }

  @Test
  void testCreateContainer() {
    BlobContainerProperties properties = mock(BlobContainerProperties.class);
    when(blobContainerClient.exists()).thenReturn(false);
    when(properties.getETag()).thenReturn("TAG");
    when(blobContainerClient.getProperties()).thenReturn(properties);

    assertThat(
        "same object is returned",
        dao.getOrCreateContainer(billingProfile, storageAccountResource).getProperties().getETag(),
        equalTo("TAG"));

    verify(blobContainerClient, times(1)).create();
  }

  @Test
  void testDeleteContainer() {
    dao.deleteContainer(billingProfile, storageAccountResource);
    verify(blobContainerClient).deleteIfExists();
  }
}
