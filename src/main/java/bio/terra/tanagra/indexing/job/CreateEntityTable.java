package bio.terra.tanagra.indexing.job;

import bio.terra.tanagra.indexing.BigQueryIndexingJob;
import bio.terra.tanagra.query.CellValue;
import bio.terra.tanagra.query.ColumnSchema;
import bio.terra.tanagra.query.QueryExecutor;
import bio.terra.tanagra.underlay.Entity;
import bio.terra.tanagra.underlay.HierarchyField;
import bio.terra.tanagra.underlay.TextSearchMapping;
import bio.terra.tanagra.underlay.Underlay;
import bio.terra.tanagra.underlay.datapointer.BigQueryDataset;
import com.google.cloud.bigquery.Field;
import com.google.cloud.bigquery.Schema;
import com.google.cloud.bigquery.TableId;
import java.util.ArrayList;
import java.util.List;

public class CreateEntityTable extends BigQueryIndexingJob {
  public CreateEntityTable(Entity entity) {
    super(entity);
  }

  @Override
  public String getName() {
    return "CREATE ENTITY TABLE (" + getEntity().getName() + ")";
  }

  @Override
  public void run(boolean isDryRun, QueryExecutor executor) {
    // Build field schemas for entity attributes.
    List<Field> fields = new ArrayList<>();
    getEntity().getAttributes().stream()
        .forEach(
            attribute ->
                attribute.getMapping(Underlay.MappingType.INDEX).buildColumnSchemas().stream()
                    .forEach(columnSchema -> fields.add(fromColumnSchema(columnSchema))));

    // Build field schemas for text mapping.
    if (getEntity().getTextSearch().isEnabled()) {
      TextSearchMapping textSearchMapping =
          getEntity().getTextSearch().getMapping(Underlay.MappingType.INDEX);
      if (textSearchMapping.definedBySearchString()
          && textSearchMapping.getTablePointer().equals(getEntityIndexTable())) {
        fields.add(
            fromColumnSchema(
                new ColumnSchema(
                    textSearchMapping.getSearchString().getColumnName(),
                    CellValue.SQLDataType.STRING)));
      }
    }

    // Build field schemas for hierarchy fields: path, num_children.
    // The other two hierarchy fields, is_root and is_member, are calculated from path.
    getEntity().getHierarchies().stream()
        .forEach(
            hierarchy -> {
              fields.add(
                  fromColumnSchema(
                      hierarchy.getField(HierarchyField.Type.PATH).buildColumnSchema()));
              fields.add(
                  fromColumnSchema(
                      hierarchy.getField(HierarchyField.Type.NUM_CHILDREN).buildColumnSchema()));
            });

    // Build field schemas for relationship fields: count, display_hints.
    getEntity().getRelationships().stream()
        .forEach(
            relationship ->
                relationship.getFields().stream()
                    .filter(relationshipField -> relationshipField.getEntity().equals(getEntity()))
                    .forEach(
                        relationshipField ->
                            fields.add(fromColumnSchema(relationshipField.buildColumnSchema()))));

    // Create an empty table with this schema.
    BigQueryDataset outputBQDataset = getBQDataPointer(getEntityIndexTable());
    TableId destinationTable =
        TableId.of(
            outputBQDataset.getProjectId(),
            outputBQDataset.getDatasetId(),
            getEntityIndexTable().getTableName());
    outputBQDataset
        .getBigQueryService()
        .createTableFromSchema(destinationTable, Schema.of(fields), isDryRun);
  }

  @Override
  public void clean(boolean isDryRun, QueryExecutor executor) {
    if (checkTableExists(getEntityIndexTable(), executor)) {
      deleteTable(getEntityIndexTable(), isDryRun);
    }
  }

  private Field fromColumnSchema(ColumnSchema columnSchema) {
    return Field.of(
        columnSchema.getColumnName(),
        BigQueryDataset.fromSqlDataType(columnSchema.getSqlDataType()));
  }

  @Override
  public JobStatus checkStatus(QueryExecutor executor) {
    // Check if the table already exists. We don't expect this to be a long-running operation, so
    // there is no IN_PROGRESS state for this job.
    return checkTableExists(getEntityIndexTable(), executor)
        ? JobStatus.COMPLETE
        : JobStatus.NOT_STARTED;
  }
}
