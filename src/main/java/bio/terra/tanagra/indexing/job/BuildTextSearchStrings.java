package bio.terra.tanagra.indexing.job;

import static bio.terra.tanagra.underlay.TextSearchMapping.TEXT_SEARCH_ID_COLUMN_NAME;
import static bio.terra.tanagra.underlay.TextSearchMapping.TEXT_SEARCH_STRING_COLUMN_NAME;

import bio.terra.tanagra.exception.SystemException;
import bio.terra.tanagra.indexing.BigQueryIndexingJob;
import bio.terra.tanagra.query.ColumnSchema;
import bio.terra.tanagra.query.FieldPointer;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.Query;
import bio.terra.tanagra.query.QueryExecutor;
import bio.terra.tanagra.underlay.Entity;
import bio.terra.tanagra.underlay.TextSearchMapping;
import bio.terra.tanagra.underlay.Underlay;
import java.util.HashMap;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BuildTextSearchStrings extends BigQueryIndexingJob {
  private static final Logger LOGGER = LoggerFactory.getLogger(BuildTextSearchStrings.class);

  public BuildTextSearchStrings(Entity entity) {
    super(entity);
  }

  @Override
  public String getName() {
    return "BUILD TEXT SEARCH (" + getEntity().getName() + ")";
  }

  @Override
  public void run(boolean isDryRun, QueryExecutor executor) {
    TextSearchMapping indexMapping =
        getEntity().getTextSearch().getMapping(Underlay.MappingType.INDEX);
    if (!indexMapping.definedBySearchString() || indexMapping.getSearchString().isForeignKey()) {
      throw new SystemException(
          "Indexing text search information only supports an index mapping to a column in the entity table");
    }

    // Build a query for the id-text pairs that we want to select.
    Query idTextPairs =
        getEntity()
            .getTextSearch()
            .getMapping(Underlay.MappingType.SOURCE)
            .queryTextSearchStrings();

    // Build a map of (output) update field name -> (input) selected FieldVariable.
    // This map only contains one item, because we're only updating the text field.
    Map<String, FieldVariable> updateFields = new HashMap<>();
    String updateTextFieldName = indexMapping.getSearchString().getColumnName();
    FieldVariable selectTextField =
        idTextPairs.getSelect().stream()
            .filter(fv -> fv.getAliasOrColumnName().equals(TEXT_SEARCH_STRING_COLUMN_NAME))
            .findFirst()
            .get();
    updateFields.put(updateTextFieldName, selectTextField);

    updateEntityTableFromSelect(
        idTextPairs, updateFields, TEXT_SEARCH_ID_COLUMN_NAME, isDryRun, executor);
  }

  @Override
  public JobStatus checkStatus(QueryExecutor executor) {
    // Check if the table already exists.
    if (!checkTableExists(getEntityIndexTable(), executor)) {
      return JobStatus.NOT_STARTED;
    }

    // Check if the table has at least 1 row where text IS NOT NULL.
    TextSearchMapping indexMapping =
        getEntity().getTextSearch().getMapping(Underlay.MappingType.INDEX);
    FieldPointer textField = indexMapping.getSearchString();
    ColumnSchema textColumnSchema = indexMapping.buildTextColumnSchema();
    return checkOneNotNullRowExists(textField, textColumnSchema, executor)
        ? JobStatus.COMPLETE
        : JobStatus.NOT_STARTED;
  }

  @Override
  public void clean(boolean isDryRun, QueryExecutor executor) {
    LOGGER.info(
        "Nothing to clean. CreateEntityTable will delete the output table, which includes all the rows updated by this job.");
  }
}
