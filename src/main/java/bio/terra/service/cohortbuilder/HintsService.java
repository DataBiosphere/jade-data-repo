package bio.terra.service.cohortbuilder;

import bio.terra.model.DisplayHintDisplayHint;
import bio.terra.model.DisplayHintEnum;
import bio.terra.model.DisplayHintEnumEnumHintValues;
import bio.terra.model.DisplayHintList;
import bio.terra.model.DisplayHintNumericRange;
import bio.terra.model.HintQuery;
import bio.terra.tanagra.query.Literal;
import bio.terra.tanagra.query.QueryRequest;
import bio.terra.tanagra.service.FromApiConversionService;
import bio.terra.tanagra.service.QuerysService;
import bio.terra.tanagra.service.UnderlaysService;
import bio.terra.tanagra.service.utils.ToApiConversionUtils;
import bio.terra.tanagra.underlay.Attribute;
import bio.terra.tanagra.underlay.DisplayHint;
import bio.terra.tanagra.underlay.Entity;
import bio.terra.tanagra.underlay.Underlay;
import bio.terra.tanagra.underlay.displayhint.EnumVals;
import bio.terra.tanagra.underlay.displayhint.NumericRange;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

@Component
public class HintsService {
  private final UnderlaysService underlaysService;
  private final QuerysService querysService;

  @Autowired
  public HintsService(UnderlaysService underlaysService, QuerysService querysService) {
    this.underlaysService = underlaysService;
    this.querysService = querysService;
  }

  public DisplayHintList queryHints(String entityId, HintQuery hintQuery) {
    Entity entity = underlaysService.getEntity(entityId);

    if (hintQuery == null || hintQuery.getRelatedEntity() == null) {
      // Return display hints computed across all entity instances (e.g. enum values for
      // person.gender).
      Map<String, bio.terra.tanagra.underlay.DisplayHint> displayHints = new HashMap<>();
      entity
          .getAttributes()
          .forEach(
              attr -> {
                if (attr.getDisplayHint() != null) {
                  displayHints.put(attr.getName(), attr.getDisplayHint());
                }
              });
      // Currently, these display hints are stored in the underlay config files, so no SQL query is
      // necessary to look them up.
      return toApiObject(entity, displayHints, null);
    } else {
      // Return display hints for entity instances that are related to an instance of another entity
      // (e.g. numeric range for measurement_occurrence.value_numeric, computed across
      // measurement_occurrence instances that are related to measurement=BodyHeight).
      Entity relatedEntity = underlaysService.getEntity(hintQuery.getRelatedEntity().getName());
      Literal relatedEntityId =
          FromApiConversionService.fromApiObject(hintQuery.getRelatedEntity().getId());
      QueryRequest queryRequest =
          querysService.buildDisplayHintsQuery(
              underlaysService.getUnderlay(UnderlaysService.UNDERLAY_NAME),
              entity,
              Underlay.MappingType.INDEX,
              relatedEntity,
              relatedEntityId);
      Map<String, DisplayHint> displayHints =
          querysService.runDisplayHintsQuery(
              entity.getMapping(Underlay.MappingType.INDEX).getTablePointer().getDataPointer(),
              queryRequest);
      return toApiObject(entity, displayHints, queryRequest.getSql());
    }
  }

  private DisplayHintList toApiObject(
      Entity entity, Map<String, DisplayHint> displayHints, String sql) {
    return new DisplayHintList()
        .sql(sql)
        .displayHints(
            displayHints.entrySet().stream()
                .map(
                    attrHint -> {
                      Attribute attr = entity.getAttribute(attrHint.getKey());
                      DisplayHint hint = attrHint.getValue();
                      return new bio.terra.model.DisplayHint()
                          .attribute(ToApiConversionUtils.toApiObject(attr))
                          .displayHint(hint == null ? null : toApiObject(hint));
                    })
                .collect(Collectors.toList()));
  }

  private DisplayHintDisplayHint toApiObject(DisplayHint displayHint) {
    return switch (displayHint.getType()) {
      case ENUM -> {
        EnumVals enumVals = (EnumVals) displayHint;
        yield new DisplayHintDisplayHint()
            .enumHint(
                new DisplayHintEnum()
                    .enumHintValues(
                        enumVals.getEnumValsList().stream()
                            .map(
                                ev ->
                                    new DisplayHintEnumEnumHintValues()
                                        .enumVal(
                                            ToApiConversionUtils.toApiObject(ev.getValueDisplay()))
                                        .count(Math.toIntExact(ev.getCount())))
                            .collect(Collectors.toList())));
      }
      case RANGE -> {
        NumericRange numericRange = (NumericRange) displayHint;
        yield new DisplayHintDisplayHint()
            .numericRangeHint(
                new DisplayHintNumericRange()
                    .min(numericRange.getMinVal())
                    .max(numericRange.getMaxVal()));
      }
    };
  }
}
