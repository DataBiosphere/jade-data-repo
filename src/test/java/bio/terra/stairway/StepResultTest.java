package bio.terra.stairway;

import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;

public class StepResultTest {
    private static String bad = "bad bad bad";


    @Test
    public void testStepResultSuccess() {
        StepResult result = StepResult.getStepResultSuccess();
        Assert.assertThat(result.getStepStatus(), is(StepStatus.STEP_RESULT_SUCCESS));
        Optional<Throwable> throwable = result.getThrowable();
        Assert.assertFalse(throwable.isPresent());
    }

    @Test
    public void testStepResultError() {
        StepResult result = new StepResult(StepStatus.STEP_RESULT_FAILURE_FATAL,
                new IllegalArgumentException(bad));
        Assert.assertThat(result.getStepStatus(), is(StepStatus.STEP_RESULT_FAILURE_FATAL));
        Optional<Throwable> throwable = result.getThrowable();
        Assert.assertTrue(throwable.isPresent());
        Assert.assertTrue(throwable.get() instanceof IllegalArgumentException);
        Assert.assertThat(throwable.get().getMessage(), is(bad));
    }

}
