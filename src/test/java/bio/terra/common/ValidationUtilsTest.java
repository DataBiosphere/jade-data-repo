package bio.terra.common;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatExceptionOfType;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.equalTo;

import bio.terra.common.category.Unit;
import bio.terra.model.ColumnModel;
import bio.terra.model.RelationshipTermModel;
import bio.terra.model.TableDataType;
import bio.terra.model.TableModel;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import liquibase.util.StringUtils;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.springframework.test.context.ActiveProfiles;

@ActiveProfiles({"google", "unittest"})
@Category(Unit.class)
public class ValidationUtilsTest {

  private TableModel personTable;
  private TableModel carTable;
  private List<TableModel> tables;

  @Test
  public void testEmailFormats() throws Exception {
    assertThat(ValidationUtils.isValidEmail("john@somewhere.com")).isTrue();
    assertThat(ValidationUtils.isValidEmail("john.foo@somewhere.com")).isTrue();
    assertThat(ValidationUtils.isValidEmail("john.foo+label@somewhere.com")).isTrue();
    assertThat(ValidationUtils.isValidEmail("john@192.168.1.10")).isTrue();
    assertThat(ValidationUtils.isValidEmail("john+label@192.168.1.10")).isTrue();
    assertThat(ValidationUtils.isValidEmail("john.foo@someserver")).isTrue();
    assertThat(ValidationUtils.isValidEmail("JOHN.FOO@somewhere.com")).isTrue();
    assertThat(ValidationUtils.isValidEmail("@someserver")).isFalse();
    assertThat(ValidationUtils.isValidEmail("@someserver.com")).isFalse();
    assertThat(ValidationUtils.isValidEmail("john@.")).isFalse();
    assertThat(ValidationUtils.isValidEmail(".@somewhere.com")).isFalse();
  }

  @Test
  public void testHasDuplicates() {
    assertThat(ValidationUtils.hasDuplicates(Arrays.asList("a", "a", "b", "c"))).isTrue();
    assertThat(ValidationUtils.hasDuplicates(Arrays.asList(1, 1, 2, 3))).isTrue();
    assertThat(ValidationUtils.hasDuplicates(Arrays.asList("a", "b", "c"))).isFalse();
    assertThat(ValidationUtils.hasDuplicates(Arrays.asList(1, 2, 3))).isFalse();
    assertThat(ValidationUtils.hasDuplicates(Collections.emptyList())).isFalse();
    assertThat(ValidationUtils.hasDuplicates(Collections.singletonList("a"))).isFalse();
  }

  @Test
  public void testDescriptionFormats() throws Exception {
    assertThat(ValidationUtils.isValidDescription("somedescription")).isTrue();
    assertThat(ValidationUtils.isValidDescription(StringUtils.repeat("X", 5_000))).isFalse();
  }

  @Test
  public void testPathFormats() throws Exception {
    assertThat(ValidationUtils.isValidPath("/some_path")).isTrue();
    assertThat(ValidationUtils.isValidPath("some_path")).isFalse();
  }

  @Test
  public void testUuidFormats() {
    assertThat(ValidationUtils.isValidUuid("2c297e7c-b303-4243-af6a-76cd9d3b0ca8")).isTrue();
    assertThat(ValidationUtils.convertToUuid("2c297e7c-b303-4243-af6a-76cd9d3b0ca8")).isPresent();
    assertThat(ValidationUtils.isValidUuid("not a uuid")).isFalse();
    assertThat(ValidationUtils.convertToUuid("not a uuid")).isEmpty();
  }

  @Test
  public void testValidationOfEmptyBlankAndNullStrings() {

    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> ValidationUtils.requireNotBlank("", "empty arg"));
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> ValidationUtils.requireNotBlank(null, "null arg"));
    assertThatExceptionOfType(IllegalArgumentException.class)
        .isThrownBy(() -> ValidationUtils.requireNotBlank("  ", "param"));
  }

  @Test
  public void testValidationOfValidInputString() {
    // no exception is thrown
    ValidationUtils.requireNotBlank("abc", "error msg");
  }

  @Test
  public void testRelationshipValidationDifferentDataType() {
    RelationshipTermModel fromTerm = new RelationshipTermModel().column("hasCat").table("person");
    RelationshipTermModel toTerm = new RelationshipTermModel().column("ownerId").table("car");
    defineSampleTables();

    LinkedHashMap errors =
        ValidationUtils.validateMatchingColumnDataTypes(fromTerm, toTerm, tables);
    assertThat("There should be one error.", errors.size(), equalTo(1));
  }

  @Test
  public void testRelationshipValidationSameDataType() {
    RelationshipTermModel fromTerm = new RelationshipTermModel().column("id").table("person");
    RelationshipTermModel toTerm = new RelationshipTermModel().column("ownerId").table("car");
    defineSampleTables();

    LinkedHashMap errors =
        ValidationUtils.validateMatchingColumnDataTypes(fromTerm, toTerm, tables);
    assertThat("The data types match so there should not be an error.", errors.size(), equalTo(0));
  }

  @Test
  public void validTermRelationship() {
    RelationshipTermModel validTerm = new RelationshipTermModel().column("id").table("person");
    defineSampleTables();
    LinkedHashMap errors = ValidationUtils.validateRelationshipTerm(validTerm, tables);
    assertThat("The term is valid so there should not be an error.", errors.size(), equalTo(0));
  }

  @Test
  public void invalidTermTable() {
    RelationshipTermModel invalidTable = new RelationshipTermModel().column("id").table("invalid");
    defineSampleTables();
    LinkedHashMap errors = ValidationUtils.validateRelationshipTerm(invalidTable, tables);
    assertThat("Invalid Table", errors.size(), equalTo(1));
  }

  @Test
  public void invalidTermColumn() {
    RelationshipTermModel invalidColumn =
        new RelationshipTermModel().column("invalid").table("person");
    defineSampleTables();
    LinkedHashMap errors = ValidationUtils.validateRelationshipTerm(invalidColumn, tables);
    assertThat("Invalid Column", errors.size(), equalTo(1));
  }

  private void defineSampleTables() {
    personTable =
        new TableModel()
            .name("person")
            .addColumnsItem(new ColumnModel().name("id").datatype(TableDataType.INTEGER))
            .addColumnsItem(new ColumnModel().name("hasCat").datatype(TableDataType.BOOLEAN));
    carTable =
        new TableModel()
            .name("car")
            .addColumnsItem(new ColumnModel().name("ownerId").datatype(TableDataType.INTEGER));
    tables = List.of(personTable, carTable);
  }
}
