package bio.terra.tanagra.query.filtervariable;

import bio.terra.model.CloudPlatform;
import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.FilterVariable;
import bio.terra.tanagra.query.Literal;
import com.google.common.collect.ImmutableMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.text.StringSubstitutor;

public class FunctionFilterVariable extends FilterVariable {
  private final FieldVariable fieldVariable;
  private final FunctionTemplate functionTemplate;
  private final List<Literal> values;

  public FunctionFilterVariable(
      FunctionTemplate functionTemplate, FieldVariable fieldVariable, Literal... values) {
    this.functionTemplate = functionTemplate;
    this.fieldVariable = fieldVariable;
    this.values = List.of(values);
  }

  @Override
  protected String getSubstitutionTemplate(CloudPlatform platform) {
    String valuesSQL =
        values.size() > 1
            ? values.stream()
                .map(literal -> literal.renderSQL(platform))
                .collect(Collectors.joining(","))
            : values.get(0).renderSQL(platform);
    Map<String, String> params =
        ImmutableMap.<String, String>builder().put("value", valuesSQL).build();
    return StringSubstitutor.replace(functionTemplate.getSqlTemplate(), params);
  }

  @Override
  public List<FieldVariable> getFieldVariables() {
    return List.of(fieldVariable);
  }

  public enum FunctionTemplate {
    TEXT_EXACT_MATCH("CONTAINS_SUBSTR(${fieldVariable}, ${value})"),
    TEXT_FUZZY_MATCH("bqutil.fn.levenshtein(UPPER(${fieldVariable}), UPPER(${value}))<5"),
    IN("${fieldVariable} IN (${value})"),
    NOT_IN("${fieldVariable} NOT IN (${value})");

    private final String sqlTemplate;

    FunctionTemplate(String sqlTemplate) {
      this.sqlTemplate = sqlTemplate;
    }

    String getSqlTemplate() {
      return sqlTemplate;
    }
  }
}
