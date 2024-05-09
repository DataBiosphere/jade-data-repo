package bio.terra.service.common;

import bio.terra.common.Relationship;
import bio.terra.common.TestUtils;
import bio.terra.service.dataset.AssetColumn;
import bio.terra.service.dataset.AssetRelationship;
import bio.terra.service.dataset.AssetSpecification;
import bio.terra.service.dataset.AssetTable;
import bio.terra.service.dataset.DatasetTable;
import bio.terra.service.tabulardata.WalkRelationship;
import java.util.List;
import java.util.UUID;

public class AssetUtils {

  public static AssetSpecification buildTestAssetSpec() {
    AssetTable assetTable_sample = setUpAssetTable("ingest-test-dataset-table-sample.json");
    AssetTable assetTable_participant =
        setUpAssetTable("ingest-test-dataset-table-participant.json");
    AssetTable assetTable_file = setUpAssetTable("ingest-test-dataset-table-file.json");

    String assetName = "sample_centric";

    return new AssetSpecification()
        .name(assetName)
        .assetTables(List.of(assetTable_file, assetTable_sample, assetTable_participant))
        .rootTable(assetTable_sample)
        .rootColumn(
            assetTable_sample.getColumns().stream()
                .filter(c -> c.getDatasetColumn().getName().equals("id"))
                .findFirst()
                .orElseThrow());
  }

  public static WalkRelationship buildExampleWalkRelationship(
      AssetSpecification assetSpecification) {
    DatasetTable participantTable =
        assetSpecification.getAssetTableByName("participant").getTable();
    DatasetTable sampleTable = assetSpecification.getAssetTableByName("sample").getTable();
    // define relationships
    AssetRelationship sampleParticipantRelationship =
        new AssetRelationship()
            .datasetRelationship(
                new Relationship()
                    .id(UUID.randomUUID())
                    .fromColumn(participantTable.getColumnByName("id").orElseThrow())
                    .fromTable(participantTable)
                    .toColumn(sampleTable.getColumnByName("participant_ids").orElseThrow())
                    .toTable(sampleTable)
                    .name("participant_sample_relationship"));
    return WalkRelationship.ofAssetRelationship(sampleParticipantRelationship);
  }

  private static AssetTable setUpAssetTable(String resourcePath) {
    DatasetTable datasetTable = TestUtils.loadObject(resourcePath, DatasetTable.class);
    datasetTable.id(UUID.randomUUID());
    List<AssetColumn> columns =
        datasetTable.getColumns().stream()
            .map(c -> new AssetColumn().datasetColumn(c).datasetTable(datasetTable))
            .toList();
    return new AssetTable().datasetTable(datasetTable).columns(columns);
  }
}
