package bio.terra.app.controller.converters;

import bio.terra.app.controller.exception.ValidationException;
import bio.terra.model.SqlSortDirection;
import org.springframework.core.convert.converter.Converter;
import org.springframework.stereotype.Component;

import java.util.Arrays;
import java.util.Collections;

@Component
public class SqlSortDirectionConverter
    implements Converter<String, SqlSortDirection> {

    @Override
    public SqlSortDirection convert(String source) {
        SqlSortDirection result = SqlSortDirection.fromValue(source.toLowerCase());
        if (result == null) {
            String error = String.format("direction must be one of: %s.", Arrays.toString(SqlSortDirection.values()));
            throw new ValidationException("Invalid enumerate parameter(s).", Collections.singletonList(error));
        }
        return result;
    }
}
