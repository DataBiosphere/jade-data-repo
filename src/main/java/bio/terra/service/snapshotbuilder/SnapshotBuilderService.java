package bio.terra.service.snapshotbuilder;

import bio.terra.common.CloudPlatformWrapper;
import bio.terra.common.Column;
import bio.terra.common.iam.AuthenticatedUserRequest;
import bio.terra.model.ColumnStatisticsDoubleModel;
import bio.terra.model.ColumnStatisticsIntModel;
import bio.terra.model.SnapshotBuilderConcept;
import bio.terra.model.SnapshotBuilderDomain;
import bio.terra.model.SnapshotBuilderDomainOption;
import bio.terra.model.SnapshotBuilderGetConceptsResponse;
import bio.terra.model.SnapshotBuilderProgramData;
import bio.terra.model.SnapshotBuilderProgramDataOption;
import bio.terra.model.SnapshotBuilderSettings;
import bio.terra.model.SnapshotBuilderSettingsOptions;
import bio.terra.service.dataset.Dataset;
import bio.terra.service.dataset.DatasetDao;
import bio.terra.service.dataset.DatasetService;
import bio.terra.service.dataset.exception.DatasetDataException;
import bio.terra.service.dataset.flight.ingest.IngestUtils;
import bio.terra.service.filedata.azure.AzureSynapsePdao;
import bio.terra.service.filedata.azure.AzureSynapseService;
import bio.terra.service.tabulardata.google.bigquery.BigQueryPdao;
import java.util.List;
import java.util.UUID;
import javax.ws.rs.BadRequestException;
import org.springframework.stereotype.Component;

@Component
public class SnapshotBuilderService {
  private final SnapshotBuilderSettingsDao snapshotBuilderSettingsDao;
  private final AzureSynapsePdao azureSynapsePdao;
  private final AzureSynapseService azureSynapseService;
  private final DatasetDao datasetDao;

  public SnapshotBuilderService(
      SnapshotBuilderSettingsDao snapshotBuilderSettingsDao,
      AzureSynapsePdao azureSynapsePdao,
      AzureSynapseService azureSynapseService,
      DatasetDao datasetDao) {
    this.snapshotBuilderSettingsDao = snapshotBuilderSettingsDao;
    this.azureSynapsePdao = azureSynapsePdao;
    this.azureSynapseService = azureSynapseService;
    this.datasetDao = datasetDao;
  }

  private SnapshotBuilderDomainOption generateDataForDomain(SnapshotBuilderDomain domain) {
    // TODO: Generate concept count and participant count.
    return new SnapshotBuilderDomainOption()
        .category(domain.getCategory())
        .id(domain.getId())
        .root(domain.getRoot());
  }

  private SnapshotBuilderSettingsOptions snapshotBuilderSettingsWithOptions(
      UUID datasetId, SnapshotBuilderSettings settings, AuthenticatedUserRequest userRequest) {
    return new SnapshotBuilderSettingsOptions()
        .domainOptions(
            settings.getSelectableDomains().stream().map(this::generateDataForDomain).toList())
        .programDataOptions(
            settings.getSelectableProgramData().stream()
                .map(
                    programData ->
                        generateTableInformationForProgramDataOption(
                            datasetId, programData, userRequest))
                .toList())
        .datasetConceptSets(settings.getPrepackagedDatasetConceptSets())
        .featureValueGroups(settings.getFeatureValueGroups());
  }

  public SnapshotBuilderSettingsOptions getSnapshotBuilderSettings(
      UUID datasetId, AuthenticatedUserRequest userRequest) {

    SnapshotBuilderSettings snapshotBuilderSettings =
        snapshotBuilderSettingsDao.getSnapshotBuilderSettingsByDatasetId(datasetId);

    return snapshotBuilderSettingsWithOptions(datasetId, snapshotBuilderSettings, userRequest);
  }

  public SnapshotBuilderProgramDataOption generateTableInformationForProgramDataOption(
      UUID datasetId,
      SnapshotBuilderProgramData programData,
      AuthenticatedUserRequest userRequest) {
    return switch (programData.getKind()) {
      case LIST -> new SnapshotBuilderProgramDataOption()
          .id(programData.getId())
          .name(programData.getName())
          .kind(SnapshotBuilderProgramDataOption.KindEnum.RANGE)
          .values(List.of());
      case RANGE -> generateRangeInformation(datasetId, programData, userRequest);
    };
  }

  public SnapshotBuilderProgramDataOption generateRangeInformation(
      UUID datasetId,
      SnapshotBuilderProgramData programDataMetadata,
      AuthenticatedUserRequest userRequest) {
    Dataset dataset = datasetDao.retrieve(datasetId);
    String tableName = "person";
    String filter = "";
    Column column = dataset.getColumn("person", programDataMetadata.getColumnName());
    SnapshotBuilderProgramDataOption response =
        new SnapshotBuilderProgramDataOption()
            .name(programDataMetadata.getName())
            .id(programDataMetadata.getId())
            .kind(SnapshotBuilderProgramDataOption.KindEnum.RANGE);
    var cloudPlatformWrapper = CloudPlatformWrapper.of(dataset.getCloudPlatform());
    try {
      if (cloudPlatformWrapper.isGcp()) {
        if (column.isDoubleType()) {
          ColumnStatisticsDoubleModel columnStatistics =
              BigQueryPdao.getStatsForDoubleColumn(dataset, tableName, column, filter);
          return response
              .min(columnStatistics.getMinValue().intValue())
              .max(columnStatistics.getMaxValue().intValue());
        } else if (column.isIntType()) {
          ColumnStatisticsIntModel columnStatistics =
              BigQueryPdao.getStatsForIntColumn(dataset, tableName, column, filter);
          return response.min(columnStatistics.getMinValue()).max(columnStatistics.getMaxValue());
        } else {
          throw new BadRequestException(
              "Snapshot builder settings not configured correctly for dataset. Should be numeric data type");
        }
      } else if (cloudPlatformWrapper.isAzure()) {
        String datasourceName =
            azureSynapseService.getOrCreateExternalAzureDataSource(dataset, userRequest);

        String sourceParquetFilePath = IngestUtils.getSourceDatasetParquetFilePath(tableName);

        if (column.isDoubleType()) {
          ColumnStatisticsDoubleModel columnStatistics =
              azureSynapsePdao.getStatsForDoubleColumn(
                  column, datasourceName, sourceParquetFilePath, filter);
          return response
              .min(columnStatistics.getMinValue().intValue())
              .max(columnStatistics.getMaxValue().intValue());
        } else if (column.isIntType()) {
          ColumnStatisticsIntModel columnStatistics =
              azureSynapsePdao.getStatsForIntColumn(
                  column, datasourceName, sourceParquetFilePath, filter);
          return response.min(columnStatistics.getMinValue()).max(columnStatistics.getMaxValue());
        } else {
          throw new BadRequestException(
              "Snapshot builder settings not configured correctly for dataset. Should be numeric data type");
        }
      } else {
        throw new DatasetDataException("Cloud not supported");
      }
    } catch (InterruptedException e) {
      throw new DatasetDataException("Error retrieving data for dataset " + dataset.getName(), e);
    }
  }

  public SnapshotBuilderSettings updateSnapshotBuilderSettings(
      UUID id, SnapshotBuilderSettings settings) {
    return snapshotBuilderSettingsDao.upsertSnapshotBuilderSettingsByDataset(id, settings);
  }

  public SnapshotBuilderGetConceptsResponse getConceptChildren(UUID datasetId, Integer conceptId) {
    // TODO: Build real query - this should get the name and ID from the concept table, the count
    // from the occurrence table, and the existence of children from the concept_ancestor table.
    return new SnapshotBuilderGetConceptsResponse()
        .result(
            List.of(
                new SnapshotBuilderConcept()
                    .count(100)
                    .name("Stub concept")
                    .hasChildren(true)
                    .id(conceptId + 1)));
  }
}
