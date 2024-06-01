package bio.terra.service.snapshotbuilder.query.table;

import static bio.terra.service.snapshotbuilder.utils.CriteriaQueryBuilderTest.assertQueryEquals;

import bio.terra.common.category.Unit;
import bio.terra.service.snapshotbuilder.query.FieldVariable;
import bio.terra.service.snapshotbuilder.query.SqlRenderContext;
import bio.terra.service.snapshotbuilder.query.SqlRenderContextProvider;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;

@Tag(Unit.TAG)
class PersonTest {

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void testAsPrimary(SqlRenderContext context) {
    Person person = Person.forPrimary();
    assertQueryEquals("person AS p", person.renderSQL(context));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void testFromColumn(SqlRenderContext context) {
    FieldVariable fieldVariable = Person.forPrimary().getColumn("column");
    assertQueryEquals("p.column", fieldVariable.renderSQL(context));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void testPersonId(SqlRenderContext context) {
    FieldVariable fieldVariable = Person.forPrimary().personId();
    assertQueryEquals("p.person_id", fieldVariable.renderSQL(context));
  }

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void testCountPersonId(SqlRenderContext context) {
    FieldVariable fieldVariable = Person.forPrimary().countPerson();
    assertQueryEquals("COUNT(DISTINCT p.person_id)", fieldVariable.renderSQL(context));
  }
}
