package bio.terra.service.load;

import java.util.UUID;
import org.hamcrest.Description;
import org.hamcrest.Matcher;
import org.hamcrest.TypeSafeMatcher;

public class LoadLockedBy extends TypeSafeMatcher<Load> {

  private final String lockingFlightId;
  private final UUID datasetId;

  public LoadLockedBy(String lockingFlightId, UUID datasetId) {
    this.lockingFlightId = lockingFlightId;
    this.datasetId = datasetId;
  }

  public static Matcher<Load> loadLockedBy(String lockingFlightId, UUID datasetId) {
    return new LoadLockedBy(lockingFlightId, datasetId);
  }

  /**
   * Subclasses should implement this. The item will already have been checked for the specific type
   * and will never be null.
   */
  @Override
  protected boolean matchesSafely(Load item) {
    return item.lockedBy(lockingFlightId, datasetId);
  }

  /**
   * Generates a description of the object. The description may be part of a description of a larger
   * object of which this is just a component, so it should be worded appropriately.
   *
   * @param description The description to be built or appended to.
   */
  @Override
  public void describeTo(Description description) {
    description.appendText("locked by supplied flight and dataset");
  }
}
