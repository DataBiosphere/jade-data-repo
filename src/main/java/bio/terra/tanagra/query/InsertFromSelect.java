package bio.terra.tanagra.query;

import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;
import org.stringtemplate.v4.ST;

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
  public String renderSQL(SqlPlatform platform) {
    // list the insert field names in the same order as the select fields
    String insertFieldsSQL =
        valueFields.entrySet().stream()
            .sorted(Comparator.comparing(p -> p.getValue().getAliasOrColumnName()))
            .map(Map.Entry::getKey)
            .collect(Collectors.joining(", "));

    String template = "INSERT INTO <insertTableSQL> (<insertFieldsSQL>) <selectQuerySQL>";
    return new ST(template)
        .add("insertTableSQL", insertTable.renderSQL(platform))
        .add("insertFieldsSQL", insertFieldsSQL)
        .add("selectQuerySQL", selectQuery.renderSQL(platform))
        .render();
  }
}
