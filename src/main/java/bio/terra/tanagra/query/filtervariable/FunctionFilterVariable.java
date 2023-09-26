package bio.terra.tanagra.query.filtervariable;

import bio.terra.tanagra.query.FieldVariable;
import bio.terra.tanagra.query.FilterVariable;
import bio.terra.tanagra.query.Literal;
import bio.terra.tanagra.query.SQLExpression;
import bio.terra.tanagra.query.SqlPlatform;
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
    if (values.length == 0) {
      throw new IllegalArgumentException("Function filter values must have at least one value");
    }
    if (values.length != 1
        && (functionTemplate == FunctionTemplate.TEXT_EXACT_MATCH
            || functionTemplate == FunctionTemplate.TEXT_FUZZY_MATCH)) {
      throw new IllegalArgumentException(
          "TEXT_EXACT_MATCH/TEXT_FUZZY_MATCH filter can only match one value");
    }
  }

  @Override
  protected String getSubstitutionTemplate(SqlPlatform platform) {
    String valuesSQL =
        values.stream()
            .map(literal -> literal.renderSQL(platform))
            .collect(Collectors.joining(","));
    Map<String, String> params = Map.of("value", valuesSQL);
    return StringSubstitutor.replace(functionTemplate.renderSQL(platform), params);
  }

  @Override
  public List<FieldVariable> getFieldVariables() {
    return List.of(fieldVariable);
  }

  public enum FunctionTemplate implements SQLExpression {
    TEXT_EXACT_MATCH("CONTAINS_SUBSTR(${fieldVariable}, ${value})"),
    TEXT_FUZZY_MATCH(
        "bqutil.fn.levenshtein(UPPER(${fieldVariable}), UPPER(${value}))<5",
        "dbo.Levenshtein(UPPER(${fieldVariable}), UPPER(${value}), 5)"),
    IN("${fieldVariable} IN (${value})"),
    NOT_IN("${fieldVariable} NOT IN (${value})");

    private final String bqSqlTemplate;
    private final String synapseSqlTemplate;

    FunctionTemplate(String template) {
      this(template, template);
    }

    FunctionTemplate(String bqSqlTemplate, String synapseSqlTemplate) {
      this.bqSqlTemplate = bqSqlTemplate;
      this.synapseSqlTemplate = synapseSqlTemplate;
    }

    @Override
    public String renderSQL(SqlPlatform platform) {
      return switch (platform) {
        case BIGQUERY -> bqSqlTemplate;
        case SYNAPSE -> synapseSqlTemplate;
      };
    }
  }
}
