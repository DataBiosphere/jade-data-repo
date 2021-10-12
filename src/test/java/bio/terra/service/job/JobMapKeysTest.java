package bio.terra.service.job;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.nullValue;

import bio.terra.common.category.Unit;
import bio.terra.stairway.FlightMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(Unit.class)
public class JobMapKeysTest {

  @Test
  public void testPutGet() {
    FlightMap map = new FlightMap();
    assertThat(JobMapKeys.DESCRIPTION.get(map), is(nullValue()));
    String value = "value";
    JobMapKeys.DESCRIPTION.put(map, value);
    assertThat(JobMapKeys.DESCRIPTION.get(map), is(value));
    assertThat(JobMapKeys.REQUEST.get(map), is(nullValue()));
  }

  public static class Model {
    public String f1 = "foo";
    public String f2 = "bar";
  }

  @Test
  public void testPutObject() {
    FlightMap map = new FlightMap();
    Model value = new Model();
    JobMapKeys.REQUEST.put(map, value);
    Model request = JobMapKeys.REQUEST.get(map);
    assertThat(request.f1, is(value.f1));
    assertThat(request.f2, is(value.f2));
  }

  @Test(expected = RuntimeException.class)
  public void testPutTypeCheck() {
    FlightMap map = new FlightMap();
    JobMapKeys.DESCRIPTION.put(map, 100);
  }
}
