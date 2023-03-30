package bio.terra.tanagra.query;

import bio.terra.model.CloudPlatform;
import java.util.HashMap;
import java.util.List;
import org.apache.commons.text.StringSubstitutor;

public abstract class FilterVariable implements SQLExpression {
  protected abstract String getSubstitutionTemplate(CloudPlatform platform);

  public abstract List<FieldVariable> getFieldVariables();

  @Override
  public String renderSQL(CloudPlatform platform) {
    HashMap<String, String> params = new HashMap<>();
    List<FieldVariable> fieldVars = getFieldVariables();
    for (int ctr = 0; ctr < fieldVars.size(); ctr++) {
      params.put("fieldVariable" + (ctr == 0 ? "" : ctr), fieldVars.get(ctr).renderSqlForWhere());
    }
    return StringSubstitutor.replace(getSubstitutionTemplate(platform), params);
  }
}
