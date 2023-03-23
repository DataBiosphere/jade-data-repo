package bio.terra.tanagra.query;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.text.StringSubstitutor;

public abstract class FilterVariable implements SQLExpression {
  protected abstract String getSubstitutionTemplate();

  public abstract List<FieldVariable> getFieldVariables();

  @Override
  public String renderSQL() {
    HashMap<String, String> params = new HashMap<>();
    List<FieldVariable> fieldVars = getFieldVariables();
    for (int ctr = 0; ctr < fieldVars.size(); ctr++) {
      params.put("fieldVariable" + (ctr == 0 ? "" : ctr), fieldVars.get(ctr).renderSqlForWhere());
    }
    return StringSubstitutor.replace(
        getSubstitutionTemplate(), Collections.unmodifiableMap(params));
  }
}
