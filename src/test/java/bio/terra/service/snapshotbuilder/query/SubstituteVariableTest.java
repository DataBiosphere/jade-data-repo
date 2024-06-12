package bio.terra.service.snapshotbuilder.query;

import static org.junit.jupiter.api.Assertions.*;

import bio.terra.common.category.Unit;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ArgumentsSource;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class SubstituteVariableTest {

  @ParameterizedTest
  @ArgumentsSource(SqlRenderContextProvider.class)
  void renderSQL(SqlRenderContext context) {
    var variable = "filter_text";
    var substituteVariable = new SubstituteVariable(variable);
    var actual = substituteVariable.renderSQL(context);
    String expected = context.getPlatform().choose("@filter_text", ":filter_text");
    assertEquals(expected, actual);
  }
}
