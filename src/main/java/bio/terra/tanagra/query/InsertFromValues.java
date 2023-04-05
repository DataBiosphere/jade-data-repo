package bio.terra.tanagra.query;

import bio.terra.model.CloudPlatform;
import com.google.common.collect.ImmutableMap;
import java.util.Collection;
import java.util.Comparator;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.apache.commons.text.StringSubstitutor;

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
  public String renderSQL(CloudPlatform platform) {
    // list the insert field names in the same order as the select fields
    String insertFieldsSQL =
        valueFields.entrySet().stream()
            .sorted(Comparator.comparing(p -> p.getValue().getAliasOrColumnName()))
            .map(Map.Entry::getKey)
            .collect(Collectors.joining(", "));

    String template = "INSERT INTO ${insertTableSQL} (${insertFieldsSQL}) VALUES ${values}";
    Map<String, String> params =
        ImmutableMap.<String, String>builder()
            .put("insertTableSQL", insertTable.renderSQL(platform))
            .put("insertFieldsSQL", insertFieldsSQL)
            .put(
                "values",
                values.stream()
                    .map(
                        rowResult ->
                            IntStream.range(0, rowResult.size())
                                .mapToObj(rowResult::get)
                                .flatMap(cellValue -> cellValue.getLiteral().stream())
                                .map(literal -> literal.renderSQL(platform))
                                .collect(Collectors.joining(",", "(", ")")))
                    .collect(Collectors.joining(",")))
            .build();
    return StringSubstitutor.replace(template, params);
  }
}
