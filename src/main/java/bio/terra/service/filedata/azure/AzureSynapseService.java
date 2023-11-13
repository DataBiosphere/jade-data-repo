package bio.terra.service.filedata.azure;

import static bio.terra.service.filedata.azure.AzureSynapsePdao.getDataSourceName;

import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.AccessInfoModel;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.resourcemanagement.MetadataDataAccessUtils;
import org.springframework.stereotype.Component;

@Component
public class AzureSynapseService {
  private final AzureSynapsePdao azureSynapsePdao;
  private final MetadataDataAccessUtils metadataDataAccessUtils;

  public AzureSynapseService(
      AzureSynapsePdao azureSynapsePdao, MetadataDataAccessUtils metadataDataAccessUtils) {
    this.azureSynapsePdao = azureSynapsePdao;
    this.metadataDataAccessUtils = metadataDataAccessUtils;
  }

  public String getOrCreateExternalAzureDataSource(
      Dataset dataset, AuthenticatedUserRequest userRequest) {
    String datasourceName = getDataSourceName(dataset.getId(), userRequest.getEmail());
    AccessInfoModel accessInfoModel =
        metadataDataAccessUtils.accessInfoFromDataset(dataset, userRequest);
    String credName = AzureSynapsePdao.getCredentialName(dataset.getId(), userRequest.getEmail());

    String metadataUrl =
        "%s?%s"
            .formatted(
                accessInfoModel.getParquet().getUrl(), accessInfoModel.getParquet().getSasToken());
    try {
      azureSynapsePdao.getOrCreateExternalDataSource(metadataUrl, credName, datasourceName);
      return datasourceName;
    } catch (Exception e) {
      throw new RuntimeException("Could not configure external datasource", e);
    }
  }
}
