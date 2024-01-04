package bio.terra.common;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertFalse;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import bio.terra.common.category.Unit;
import bio.terra.stairway.FlightContext;
import bio.terra.stairway.FlightMap;
import bio.terra.stairway.Step;
import bio.terra.stairway.StepResult;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(Unit.class)
public class StepUtilsTest {

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
    when(context.getInputParameters()).thenReturn(new FlightMap());
    return context;
  }

  // flip read/write sense in test names
  // test for base class annotations
  // add test for reading from inputParameters as well as flightMap
  @Test
  public void testReadInputs() {
    FlightContext context = createContext();
    context.getInputParameters().put("description", DESCRIPTION);
    context.getWorkingMap().put("datasetId", DATASET_UUID);
    Step1 step1 = new Step1();
    StepUtils.readInputs(step1, context);
    assertThat(step1.description, is(DESCRIPTION));
    assertThat(step1.datasetId, is(DATASET_UUID));
  }

  @Test
  public void testWriteOutputs() throws JsonProcessingException {
    FlightContext context = createContext();
    Step1 step1 = new Step1();
    step1.doStep(context);
    StepUtils.writeOutputs(step1, context);
    FlightMap map = context.getWorkingMap();
    List<FileModel> files = map.get("fileIds", new TypeReference<>() {});
    assertThat(files, is(FILE_IDS));
    assertThat(map.get("result", ResultModel.class).field, is(RESULT.field));
  }

  @Test(expected = RuntimeException.class)
  public void testInputTypeMismatch() {
    FlightContext context = createContext();
    context.getInputParameters().put("description", DESCRIPTION);
    context.getInputParameters().put("datasetId", 123);
    Step1 step1 = new Step1();
    StepUtils.readInputs(step1, context);
  }

  @Test(expected = RuntimeException.class)
  public void testMissingInputs() {
    FlightContext context = createContext();
    context.getInputParameters().put("description", DESCRIPTION);
    Step1 step1 = new Step1();
    StepUtils.readInputs(step1, context);
  }
  // missing outputs, can't verify
  // mistyped outputs, can't verify

  @Test
  public void testSkipNullOutput() {
    FlightContext context = createContext();
    Step1 step1 = new Step1();
    StepUtils.writeOutputs(step1, context);
    assertFalse(context.getWorkingMap().containsKey("description"));
  }
}
