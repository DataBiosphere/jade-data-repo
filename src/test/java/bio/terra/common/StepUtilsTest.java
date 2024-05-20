package bio.terra.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import bio.terra.stairway.exception.JsonConversionException;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.type.TypeReference;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.jupiter.api.Tag;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;

@SuppressFBWarnings({
  "URF_UNREAD_PUBLIC_OR_PROTECTED_FIELD",
  "URF_UNREAD_FIELD",
  "HE_EQUALS_USE_HASHCODE"
})
@ExtendWith(MockitoExtension.class)
@Tag(Unit.TAG)
class StepUtilsTest {

  private static final String DESCRIPTION = "a description";
  private static final UUID DATASET_UUID = UUID.randomUUID();
  private static final List<FileModel> FILE_IDS =
      Arrays.asList(new FileModel("file1"), new FileModel("file2"));
  private static final ResultModel RESULT = new ResultModel("value");

  public static class FileModel {
    @JsonProperty String name;

    public FileModel() {}

    public FileModel(String name) {
      this.name = name;
    }

    @Override
    public boolean equals(Object obj) {
      return obj instanceof FileModel && ((FileModel) obj).name.equals(name);
    }
  }

  public static class ResultModel {
    @JsonProperty String field;

    public ResultModel() {}

    public ResultModel(String field) {
      this.field = field;
    }
  }

  abstract static class Base implements Step {
    @StepInput protected String description;
    @StepOutput protected ResultModel result;
  }

  static class Step1 extends Base {

    @StepInput private UUID datasetId;

    @StepOutput private List<FileModel> fileIds;

    @Override
    public StepResult doStep(FlightContext context) {
      fileIds = FILE_IDS;
      result = RESULT;
      return null;
    }

    @Override
    public StepResult undoStep(FlightContext context) {
      return null;
    }
  }

  static FlightContext createContext() {
    FlightContext context = mock(FlightContext.class);
    when(context.getWorkingMap()).thenReturn(new FlightMap());
    return context;
  }

  // flip read/write sense in test names
  // test for base class annotations
  // add test for reading from inputParameters as well as flightMap
  @Test
  void testReadInputs() {
    FlightContext context = createContext();
    when(context.getInputParameters()).thenReturn(new FlightMap());
    context.getInputParameters().put("description", DESCRIPTION);
    context.getWorkingMap().put("datasetId", DATASET_UUID);
    Step1 step1 = new Step1();
    StepUtils.readInputs(step1, context);
    assertThat(step1.description, is(DESCRIPTION));
    assertThat(step1.datasetId, is(DATASET_UUID));
  }

  @Test
  void testWriteOutputs() {
    FlightContext context = createContext();
    Step1 step1 = new Step1();
    step1.doStep(context);
    StepUtils.writeOutputs(step1, context);
    FlightMap map = context.getWorkingMap();
    List<FileModel> files = map.get("fileIds", new TypeReference<>() {});
    assertThat(files, is(FILE_IDS));
    assertThat(map.get("result", ResultModel.class).field, is(RESULT.field));
  }

  @Test
  void testInputTypeMismatch() {
    FlightContext context = mock(FlightContext.class);
    when(context.getInputParameters()).thenReturn(new FlightMap());
    context.getInputParameters().put("description", DESCRIPTION);
    context.getInputParameters().put("datasetId", 123);
    Step1 step1 = new Step1();
    assertThrows(JsonConversionException.class, () -> StepUtils.readInputs(step1, context));
  }

  @Test
  void testMissingInputs() {
    FlightContext context = createContext();
    when(context.getInputParameters()).thenReturn(new FlightMap());
    context.getInputParameters().put("description", DESCRIPTION);
    Step1 step1 = new Step1();
    assertThrows(
        StepUtils.MissingStepInputException.class, () -> StepUtils.readInputs(step1, context));
  }
  // missing outputs, can't verify
  // mistyped outputs, can't verify

  @Test
  void testSkipNullOutput() {
    FlightContext context = createContext();
    Step1 step1 = new Step1();
    StepUtils.writeOutputs(step1, context);
    assertFalse(context.getWorkingMap().containsKey("description"));
  }

  static class Step3 implements Step {
    @StepInput("aaa")
    private String thingOne;

    @StepOutput("bbb")
    private String thingTwo;

    @Override
    public StepResult doStep(FlightContext context) {
      thingTwo = "two";
      return null;
    }

    @Override
    public StepResult undoStep(FlightContext context) {
      return null;
    }
  }

  @Test
  void testNamedInputOutput() {
    FlightContext context = createContext();
    when(context.getInputParameters()).thenReturn(new FlightMap());
    context.getWorkingMap().put("aaa", "one");
    Step3 step3 = new Step3();
    StepUtils.readInputs(step3, context);
    step3.doStep(context);
    StepUtils.writeOutputs(step3, context);
    assertThat(step3.thingOne, is("one"));
    assertThat(context.getWorkingMap().get("bbb", String.class), is("two"));
  }
}
