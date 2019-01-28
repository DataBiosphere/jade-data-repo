package bio.terra.stairway;

import bio.terra.category.StairwayUnit;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Optional;

import static org.hamcrest.CoreMatchers.is;

@Category(StairwayUnit.class)
public class FlightResultTest {
    private static String bad = "bad bad bad";
    private FlightResult result;

    @Before
    public void setup() {
        result = FlightResult.flightResultFatal(new IllegalArgumentException(bad));
    }

    @Test
    public void testFlightResultFatal() {
        Assert.assertFalse(result.isSuccess());
        Optional<Throwable> throwable = result.getThrowable();
        Assert.assertTrue(throwable.isPresent());
        Assert.assertTrue(throwable.get() instanceof IllegalArgumentException);
        Assert.assertThat(throwable.get().getMessage(), is(bad));
    }

    @Test(expected = UnsupportedOperationException.class)
    public void testResultMapIsImmutable() {
        result.getResultMap().put(bad, bad);
    }

}
