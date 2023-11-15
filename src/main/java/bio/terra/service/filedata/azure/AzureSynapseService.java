package bio.terra.service.filedata.azure;

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
}
