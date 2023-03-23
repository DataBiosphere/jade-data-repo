package bio.terra.tanagra.serialization.filter;

import bio.terra.tanagra.query.Filter;
import bio.terra.tanagra.query.TablePointer;
import bio.terra.tanagra.query.filter.BooleanAndOrFilter;
import bio.terra.tanagra.query.filtervariable.BooleanAndOrFilterVariable;
import bio.terra.tanagra.serialization.UFFilter;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import java.util.List;
import java.util.stream.Collectors;

/**
 * External representation of an array table filter: subfilter1 operator subfilter2 operator ...
 * (e.g. domain_id = "Condition" AND is_expired = FALSE).
 *
 * <p>This is a POJO class intended for serialization. This JSON format is user-facing.
 */
@JsonDeserialize(builder = UFBooleanAndOrFilter.Builder.class)
public class UFBooleanAndOrFilter extends UFFilter {
  private final BooleanAndOrFilterVariable.LogicalOperator operator;
  private final List<UFFilter> subfilters;

  public UFBooleanAndOrFilter(BooleanAndOrFilter booleanAndOrFilter) {
    super(booleanAndOrFilter);
    this.operator = booleanAndOrFilter.getOperator();
    this.subfilters =
        booleanAndOrFilter.getSubfilters().stream()
            .map(sf -> sf.serialize())
            .collect(Collectors.toList());
  }

  private UFBooleanAndOrFilter(Builder builder) {
    super(builder);
    this.operator = builder.operator;
    this.subfilters = builder.subfilters;
  }

  @JsonPOJOBuilder(buildMethodName = "build", withPrefix = "")
  public static class Builder extends UFFilter.Builder {
    private BooleanAndOrFilterVariable.LogicalOperator operator;
    private List<UFFilter> subfilters;

    public Builder operator(BooleanAndOrFilterVariable.LogicalOperator operator) {
      this.operator = operator;
      return this;
    }

    public Builder subfilters(List<UFFilter> subfilters) {
      this.subfilters = subfilters;
      return this;
    }

    /** Call the private constructor. */
    @Override
    public UFBooleanAndOrFilter build() {
      return new UFBooleanAndOrFilter(this);
    }
  }

  /** Deserialize to the internal representation of the table filter. */
  @Override
  public Filter deserializeToInternal(TablePointer tablePointer) {
    return BooleanAndOrFilter.fromSerialized(this, tablePointer);
  }

  public BooleanAndOrFilterVariable.LogicalOperator getOperator() {
    return operator;
  }

  public List<UFFilter> getSubfilters() {
    return subfilters;
  }
}
