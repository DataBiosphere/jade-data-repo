package bio.terra.app.controller.converters;

import bio.terra.model.SqlSortDirectionAscDefault;
import java.util.Arrays;
import org.springframework.stereotype.Component;

@Component
public class SqlSortDirectionAscDefaultConverter
    extends OpenApiEnumConverter<SqlSortDirectionAscDefault> {

  @Override
  SqlSortDirectionAscDefault fromValue(String source) {
    return SqlSortDirectionAscDefault.fromValue(source);
  }

  @Override
  String errorString() {
    return String.format(
        "direction must be one of: %s.", Arrays.toString(SqlSortDirectionAscDefault.values()));
  }
}
