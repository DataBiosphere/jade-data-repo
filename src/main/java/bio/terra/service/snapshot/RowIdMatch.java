package bio.terra.service.snapshot;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * RowIdMatch provides three lists:
 * <ol>
 *  <li>A list of the input values that matched</li>
 *  <li>A parallel list of the corresponding row ids of the matches</li>
 *  <li>A list of the input values that did not match</li>
 * </ol>
 *
 * This can be converted to a model to return when there are errors with the input
 * values.
 */
public class RowIdMatch {
    private List<String> matchedInputValues;
    private List<String> matchingRowIds;
    private List<String> unmatchedInputValues;

    public RowIdMatch() {
        matchedInputValues = new ArrayList<>();
        matchingRowIds = new ArrayList<>();
        unmatchedInputValues = new ArrayList<>();
    }

    public RowIdMatch addMatch(String inputValue, String rowId) {
        matchedInputValues.add(inputValue);
        matchingRowIds.add(rowId);
        return this;
    }

    public RowIdMatch addMismatch(String inputValue) {
        unmatchedInputValues.add(inputValue);
        return this;
    }

    public List<String> getMatchedInputValues() {
        return Collections.unmodifiableList(matchedInputValues);
    }

    public List<String> getMatchingRowIds() {
        return Collections.unmodifiableList(matchingRowIds);
    }

    public List<String> getUnmatchedInputValues() {
        return Collections.unmodifiableList(unmatchedInputValues);
    }
}
