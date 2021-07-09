package bio.terra.datarepo.service.resourcemanagement.azure;

import bio.terra.datarepo.common.exception.PdaoException;
import bio.terra.datarepo.model.BillingProfileModel;
import com.azure.storage.file.datalake.DataLakeFileSystemClient;
import com.azure.storage.file.datalake.DataLakeServiceClient;
import com.azure.storage.file.datalake.models.DataLakeStorageException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Component;

@Component
public class AzureContainerPdao {

  private final AzureAuthService authService;

  @Autowired
  public AzureContainerPdao(AzureAuthService authService) {
    this.authService = authService;
  }

  /**
   * Get or create a container (also known as FileSystem) in an Azure storage account
   *
   * @param profileModel The profile that describes information needed to access the storage account
   * @param storageAccountResource Metadata describing the storage account that contains the
   *     container
   * @param containerType The nature of the container
   * @return A client connection object that can be used to create folders and files
   */
  public DataLakeFileSystemClient getOrCreateContainer(
      BillingProfileModel profileModel,
      AzureStorageAccountResource storageAccountResource,
      AzureStorageAccountResource.ContainerType containerType) {
    DataLakeServiceClient dataLakeClient =
        authService.getDataLakeClient(profileModel, storageAccountResource);

    try {
      DataLakeFileSystemClient fileSystemClient =
          dataLakeClient.getFileSystemClient(
              storageAccountResource.determineContainer(containerType));
      // This will 404 if the container doesn't exist
      fileSystemClient.getProperties();
      return fileSystemClient;
    } catch (DataLakeStorageException e) {
      if (e.getStatusCode() == HttpStatus.NOT_FOUND.value()) {
        return dataLakeClient.createFileSystem(
            storageAccountResource.determineContainer(containerType));
      }
      throw new PdaoException("Could not create a Storage account container");
    }
  }
}
