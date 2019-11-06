package bio.terra.stairway;

import bio.terra.common.category.StairwayUnit;
import org.hamcrest.CoreMatchers;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;

@Category(StairwayUnit.class)
public class StepResultTest {
    private static String bad = "bad bad bad";

    @Test
    public void testStepResultSuccess() {
        StepResult result = StepResult.getStepResultSuccess();
        Assert.assertThat(result.getStepStatus(), CoreMatchers.is(StepStatus.STEP_RESULT_SUCCESS));
        Optional<Exception> exception = result.getException();
        Assert.assertFalse(exception.isPresent());
    }

    @Test
    public void testStepResultError() {
        StepResult result = new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL,
                new IllegalArgumentException(bad));
        Assert.assertThat(result.getStepStatus(), is(StepStatus.STEP_RESULT_FAILURE_FATAL));
        Optional<Exception> exception = result.getException();
        Assert.assertTrue(exception.isPresent());
        Assert.assertTrue(exception.get() instanceof IllegalArgumentException);
        Assert.assertThat(exception.get().getMessage(), is(bad));
    }

}
