package bio.terra.tanagra.query;

import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.stringtemplate.v4.ST;

public class InsertFromValues implements SQLExpression {
  private final TableVariable insertTable;
  private final Map<String, FieldVariable> valueFields;
  private final Collection<RowResult> values;

  public InsertFromValues(
      TableVariable insertTable,
      Map<String, FieldVariable> valueFields,
      Collection<RowResult> values) {
    this.insertTable = insertTable;
    this.valueFields = valueFields;
    this.values = values;
  }

  @Override
  public String renderSQL(SqlPlatform platform) {
    // list the insert field names in the same order as the select fields
    List<String> sortedColumns =
        valueFields.entrySet().stream()
            .sorted(Comparator.comparing(p -> p.getValue().getAliasOrColumnName()))
            .map(Map.Entry::getKey)
            .toList();

    String insertFieldsSQL = String.join(", ", sortedColumns);

    String template = "INSERT INTO <insertTableSQL> (<insertFieldsSQL>) VALUES <values>";
    return new ST(template)
        .add("insertTableSQL", insertTable.renderSQL(platform))
        .add("insertFieldsSQL", insertFieldsSQL)
        .add(
            "values",
            values.stream()
                .map(
                    rowResult ->
                        sortedColumns.stream()
                            .map(rowResult::get)
                            .map(
                                cellValue ->
                                    cellValue.getLiteral().orElseGet(() -> new Literal(null)))
                            .map(literal -> literal.renderSQL(platform))
                            .collect(Collectors.joining(",", "(", ")")))
                .collect(Collectors.joining(",")))
        .render();
  }
}
