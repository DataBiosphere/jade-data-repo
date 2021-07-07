package bio.terra.service.resourcemanagement.azure;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.common.exception.PdaoException;
import bio.terra.model.BillingProfileModel;
import bio.terra.service.resourcemanagement.azure.AzureStorageAccountResource.ContainerType;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import com.azure.storage.file.datalake.models.FileSystemProperties;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.springframework.http.HttpStatus;

@RunWith(MockitoJUnitRunner.class)
@Category(Unit.class)
public class AzureContainerPdaoTest {

  @Mock private AzureAuthService authService;
  @Mock private DataLakeServiceClient dataLakeServiceClient;
  private AzureStorageAccountResource storageAccountResource;
  private BillingProfileModel billingProfile;
  private AzureContainerPdao dao;

  @Before
  public void setUp() throws Exception {
    when(authService.getDataLakeClient(any(), any())).thenReturn(dataLakeServiceClient);

    billingProfile = new BillingProfileModel();
    storageAccountResource =
        new AzureStorageAccountResource()
            .name("mystorageaccount")
            .metadataContainer("md")
            .dataContainer("d");

    dao = new AzureContainerPdao(authService);
  }

  @Test
  public void testGetContainer() {
    DataLakeFileSystemClient dataLakeFileSystemClient = mock(DataLakeFileSystemClient.class);
    FileSystemProperties properties = mock(FileSystemProperties.class);
    when(properties.getETag()).thenReturn("TAG");
    when(dataLakeFileSystemClient.getProperties()).thenReturn(properties);
    when(dataLakeServiceClient.getFileSystemClient("d")).thenReturn(dataLakeFileSystemClient);

    DataLakeFileSystemClient returnedClient =
        dao.getOrCreateContainer(billingProfile, storageAccountResource, ContainerType.DATA);

    assertThat(returnedClient.getProperties().getETag(), equalTo("TAG"));
    verify(dataLakeServiceClient, times(0)).createFileSystem(any());
  }

  @Test
  public void testCreateContainer() {
    DataLakeFileSystemClient dataLakeFileSystemClient = mock(DataLakeFileSystemClient.class);
    DataLakeStorageException exception = mock(DataLakeStorageException.class);
    when(exception.getStatusCode()).thenReturn(HttpStatus.NOT_FOUND.value());
    when(dataLakeFileSystemClient.getProperties()).thenThrow(exception);
    when(dataLakeServiceClient.getFileSystemClient("d")).thenReturn(dataLakeFileSystemClient);

    dao.getOrCreateContainer(billingProfile, storageAccountResource, ContainerType.DATA);

    verify(dataLakeServiceClient, times(1)).createFileSystem(eq("d"));
  }

  @Test
  public void testGetOrCreateContainerNoPermissions() {
    DataLakeFileSystemClient dataLakeFileSystemClient = mock(DataLakeFileSystemClient.class);
    DataLakeStorageException exception = mock(DataLakeStorageException.class);
    when(exception.getStatusCode()).thenReturn(HttpStatus.FORBIDDEN.value());
    when(dataLakeFileSystemClient.getProperties()).thenThrow(exception);
    when(dataLakeServiceClient.getFileSystemClient("md")).thenReturn(dataLakeFileSystemClient);

    assertThrows(
        PdaoException.class,
        () ->
            dao.getOrCreateContainer(
                billingProfile, storageAccountResource, ContainerType.METADATA),
        "Invalid failure causes real error");
  }
}
