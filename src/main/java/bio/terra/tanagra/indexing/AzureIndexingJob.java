package bio.terra.tanagra.indexing;

import bio.terra.tanagra.exception.InvalidConfigException;
import bio.terra.tanagra.query.ColumnSchema;
import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.underlay.DataPointer;
import bio.terra.tanagra.underlay.Entity;
import bio.terra.tanagra.underlay.Underlay;
import bio.terra.tanagra.underlay.datapointer.AzureDataset;
import com.google.common.annotations.VisibleForTesting;
import org.apache.commons.lang3.NotImplementedException;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

public abstract class AzureIndexingJob implements IndexingJob {
  private static final DateTimeFormatter FORMATTER =
      DateTimeFormat.forPattern("MMddHHmm").withZone(DateTimeZone.UTC);

  protected static final String DEFAULT_REGION = "us-central1";

  private final Entity entity;

  protected AzureIndexingJob(Entity entity) {
    this.entity = entity;
  }

  protected Entity getEntity() {
    return entity;
  }

  @VisibleForTesting
  public TablePointer getEntityIndexTable() {
    return getEntity().getMapping(Underlay.MappingType.INDEX).getTablePointer();
  }

  protected AzureDataset getAzureDataPointer(TablePointer tablePointer) {
    DataPointer outputDataPointer = tablePointer.getDataPointer();
    if (!(outputDataPointer instanceof AzureDataset)) {
      throw new InvalidConfigException("Entity indexing job only supports BigQuery");
    }
    return (AzureDataset) outputDataPointer;
  }

  protected void deleteTable(TablePointer tablePointer, boolean isDryRun) {
    throw new NotImplementedException();
  }

  // -----Helper methods for checking whether a job has run already.-------
  protected boolean checkTableExists(TablePointer tablePointer) {
    throw new NotImplementedException();
  }

  protected boolean checkOneNotNullIdRowExists(Entity entity) {
    // Check if the table has at least 1 id row where id IS NOT NULL
    FieldPointer idField =
        getEntity().getIdAttribute().getMapping(Underlay.MappingType.INDEX).getValue();
    ColumnSchema idColumnSchema =
        getEntity()
            .getIdAttribute()
            .getMapping(Underlay.MappingType.INDEX)
            .buildValueColumnSchema();
    return checkOneNotNullRowExists(idField, idColumnSchema);
  }

  protected boolean checkOneNotNullRowExists(FieldPointer fieldPointer, ColumnSchema columnSchema) {
    throw new NotImplementedException();
  }
}
