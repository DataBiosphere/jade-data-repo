package bio.terra.datarepo.common.fixtures;

import bio.terra.datarepo.common.Column;
import bio.terra.datarepo.model.AssetModel;
import bio.terra.datarepo.model.AssetTableModel;
import bio.terra.datarepo.model.ColumnModel;
import bio.terra.datarepo.model.DatasetRequestModel;
import bio.terra.datarepo.model.DatasetSpecificationModel;
import bio.terra.datarepo.model.DatasetSummaryModel;
import bio.terra.datarepo.model.RelationshipModel;
import bio.terra.datarepo.model.RelationshipTermModel;
import bio.terra.datarepo.model.TableDataType;
import bio.terra.datarepo.model.TableModel;
import bio.terra.datarepo.service.dataset.DatasetTable;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

public final class DatasetFixtures {
  private DatasetFixtures() {}

  // TODO: everything below this line is used in exactly one place and it can be replaced with
  //  the method we use for other JSON tests: reading a resource file.
  public static DatasetSummaryModel buildMinimalDatasetSummary() {
    return new DatasetSummaryModel()
        .id(UUID.randomUUID())
        .name("Minimal")
        .description("This is a sample dataset definition");
  }

  public static TableModel buildParticipantTable() {
    return new TableModel()
        .name("participant")
        .columns(
            Arrays.asList(
                new ColumnModel().name("id").datatype(TableDataType.STRING),
                new ColumnModel().name("age").datatype(TableDataType.INTEGER)));
  }

  public static TableModel buildSampleTable() {
    return new TableModel()
        .name("sample")
        .columns(
            Arrays.asList(
                new ColumnModel().name("id").datatype(TableDataType.STRING),
                new ColumnModel().name("participant_id").datatype(TableDataType.STRING),
                new ColumnModel().name("date_collected").datatype(TableDataType.DATE)));
  }

  public static RelationshipTermModel buildParticipantTerm() {
    return new RelationshipTermModel().table("participant").column("id");
  }

  public static RelationshipTermModel buildSampleTerm() {
    return new RelationshipTermModel().table("sample").column("participant_id");
  }

  public static RelationshipModel buildParticipantSampleRelationship() {
    return new RelationshipModel()
        .name("participant_sample")
        .from(buildParticipantTerm())
        .to(buildSampleTerm());
  }

  public static AssetTableModel buildAssetParticipantTable() {
    return new AssetTableModel().name("participant").columns(Collections.emptyList());
  }

  public static AssetTableModel buildAssetSampleTable() {
    return new AssetTableModel()
        .name("sample")
        .columns(Arrays.asList("participant_id", "date_collected"));
  }

  public static AssetModel buildAsset() {
    return new AssetModel()
        .name("sample")
        .rootTable("sample")
        .rootColumn("participant_id")
        .tables(Arrays.asList(buildAssetParticipantTable(), buildAssetSampleTable()))
        .follow(Collections.singletonList("participant_sample"));
  }

  public static DatasetSpecificationModel buildSchema() {
    return new DatasetSpecificationModel()
        .tables(Arrays.asList(buildParticipantTable(), buildSampleTable()))
        .relationships(Collections.singletonList(buildParticipantSampleRelationship()))
        .assets(Collections.singletonList(buildAsset()));
  }

  public static DatasetRequestModel buildDatasetRequest() {
    return new DatasetRequestModel()
        .name("Minimal")
        .description("This is a sample dataset definition")
        .defaultProfileId(UUID.randomUUID())
        .schema(buildSchema());
  }

  /** Use method to generate a DatasetTable. Individual values can be mutated by tests if needed */
  public static DatasetTable generateDatasetTable(
      String name, TableDataType baseType, List<String> columnNames) {
    if (columnNames.isEmpty()) {
      throw new RuntimeException("Number of columns must be greater than 0");
    }
    List<Column> columns =
        columnNames.stream()
            .map(c -> new Column().id(UUID.randomUUID()).name(c).arrayOf(false).type(baseType))
            .collect(Collectors.toList());
    DatasetTable datasetTable =
        new DatasetTable()
            .id(UUID.randomUUID())
            .name(name)
            .rawTableName(name)
            .columns(columns)
            .primaryKey(columns.subList(0, 0));
    datasetTable.getColumns().forEach(c -> c.table(datasetTable));

    return datasetTable;
  }
}
