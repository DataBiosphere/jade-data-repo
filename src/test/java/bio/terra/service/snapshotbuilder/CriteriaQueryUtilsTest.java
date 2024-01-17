package bio.terra.service.snapshotbuilder;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import bio.terra.model.SnapshotBuilderCriteria;
import bio.terra.model.SnapshotBuilderProgramDataListCriteria;
import bio.terra.model.SnapshotBuilderProgramDataRangeCriteria;
import bio.terra.service.snapshotbuilder.query.FilterVariable;
import bio.terra.service.snapshotbuilder.query.TablePointer;
import bio.terra.service.snapshotbuilder.query.TableVariable;
import java.math.BigDecimal;
import java.util.List;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
public class CriteriaQueryUtilsTest {

  @Test
  void generateRangeCriteriaFilterProducesCorrectSql() {
    TablePointer tablePointer = TablePointer.fromTableName("person");
    TableVariable tableVariable = TableVariable.forPrimary(tablePointer);

    SnapshotBuilderProgramDataRangeCriteria rangeCriteria =
        (SnapshotBuilderProgramDataRangeCriteria)
            new SnapshotBuilderProgramDataRangeCriteria()
                .low(new BigDecimal(10))
                .high(new BigDecimal(50))
                .id(new BigDecimal(0))
                .name("column_name")
                .kind(SnapshotBuilderCriteria.KindEnum.RANGE);
    FilterVariable filterVariable =
        CriteriaQueryUtils.generateFilterForRangeCriteria(rangeCriteria);

    // Table name is null because there is no alias generated
    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(),
        equalTo("(null.column_name >= FLOAT('10.0') AND null.column_name <= FLOAT('50.0'))"));
    assertThat(
        "The filter just has person as the table",
        filterVariable.getTables().stream()
            .map(TableVariable::getTablePointer)
            .map(TablePointer::tableName)
            .toList(),
        equalTo(List.of("person")));
  }

  @Test
  void generateListCriteriaFilterProducesCorrectSql() {
    SnapshotBuilderProgramDataListCriteria listCriteria =
        (SnapshotBuilderProgramDataListCriteria)
            new SnapshotBuilderProgramDataListCriteria()
                .values(List.of(new BigDecimal(0), new BigDecimal(1), new BigDecimal(2)))
                .id(new BigDecimal(0))
                .name("column_name")
                .kind(SnapshotBuilderCriteria.KindEnum.LIST);
    FilterVariable filterVariable = CriteriaQueryUtils.generateFilterForListCriteria(listCriteria);

    // Table name is null because there is no alias generated
    assertThat(
        "The sql generated is correct",
        filterVariable.renderSQL(),
        equalTo("null.concept_id IN (FLOAT('0.0'),FLOAT('1.0'),FLOAT('2.0'))"));
    // The person table is reachable by traversing through the table variables, but isn't directly
    // available
    assertThat(
        "The filter just has the concept table, but not the person table",
        filterVariable.getTables().stream()
            .map(TableVariable::getTablePointer)
            .map(TablePointer::tableName)
            .toList(),
        equalTo(List.of("concept")));
  }
}
