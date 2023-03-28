package bio.terra.tanagra.query;

import com.google.common.collect.ImmutableMap;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.text.StringSubstitutor;

public class InsertFromSelect implements SQLExpression {
  private final TableVariable insertTable;
  private final Map<String, FieldVariable> valueFields;
  private final Query selectQuery;

  public InsertFromSelect(
      TableVariable insertTable, Map<String, FieldVariable> valueFields, Query selectQuery) {
    this.insertTable = insertTable;
    this.valueFields = valueFields;
    this.selectQuery = selectQuery;
  }

  @Override
  public String renderSQL() {
    // list the insert field names in the same order as the select fields
    String insertFieldsSQL =
        valueFields.entrySet().stream()
            .sorted(Comparator.comparing(p -> p.getValue().getAliasOrColumnName()))
            .map(valueField -> valueField.getKey())
            .collect(Collectors.joining(", "));

    String template = "INSERT INTO ${insertTableSQL} (${insertFieldsSQL}) ${selectQuerySQL}";
    Map<String, String> params =
        ImmutableMap.<String, String>builder()
            .put("insertTableSQL", insertTable.renderSQL())
            .put("insertFieldsSQL", insertFieldsSQL)
            .put("selectQuerySQL", selectQuery.renderSQL())
            .build();
    return StringSubstitutor.replace(template, params);
  }
}
