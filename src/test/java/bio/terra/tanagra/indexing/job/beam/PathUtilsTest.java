package bio.terra.tanagra.indexing.job.beam;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

import bio.terra.common.category.Unit;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(Unit.class)
public class PathUtilsTest {

  public static final Map<Long, Long> NO_SIBLINGS_EXPECTED_NUM_CHILDREN =
      Map.of(
          21L, 1L,
          31L, 1L,
          22L, 1L);
  public static final Map<Long, Long> HAS_SIBLINGS_EXPECTED_NUM_CHILDREN =
      Map.of(
          21L, 2L,
          31L, 2L,
          22L, 1L);
  public static final List<Pair<Long, List<Long>>> HAS_SIBLINGS_EXPECTED_COMPUTE_PATHS =
      List.of(
          Pair.of(10L, List.of(21L, 31L)),
          Pair.of(11L, List.of(21L, 31L)),
          Pair.of(20L, List.of(31L)),
          Pair.of(21L, List.of(31L)),
          Pair.of(31L, List.of()),
          Pair.of(12L, List.of(22L)),
          Pair.of(22L, List.of()),
          Pair.of(13L, List.of()));
  public static final List<Pair<Long, List<Long>>> NO_SIBLINGS_EXPECTED_COMPUTE_PATHS =
      List.of(
          Pair.of(11L, List.of(21L, 31L)),
          Pair.of(21L, List.of(31L)),
          Pair.of(31L, List.of()),
          Pair.of(12L, List.of(22L)),
          Pair.of(22L, List.of()),
          Pair.of(13L, List.of()));
  // list of all nodes for a graph with only one child per parent
  private static final List<Long> NO_SIBLINGS_ALLNODES = List.of(11L, 21L, 31L, 12L, 22L, 13L);

  // list of parent-child relationships for a graph with only one child per parent
  private static final List<Pair<Long, Long>> NO_SIBLINGS_PARENT_CHILD_RELATIONSHIPS =
      List.of(Pair.of(11L, 21L), Pair.of(21L, 31L), Pair.of(12L, 22L));

  // maximum path length for a graph with only one child per parent
  private static final int NO_SIBLINGS_MAXPATHLENGTH = 4;

  // list of all nodes for a graph with multiple children per parent
  private static final List<Long> HAS_SIBLINGS_ALLNODES =
      List.of(10L, 11L, 20L, 21L, 31L, 12L, 22L, 13L);
  public static final List<Pair<Long, Long>> HAS_SIBLINGS_PARENT_CHILD_RELATIONSHIPS =
      List.of(
          Pair.of(10L, 21L),
          Pair.of(11L, 21L),
          Pair.of(20L, 31L),
          Pair.of(21L, 31L),
          Pair.of(12L, 22L));

  // list of parent-child relationships for a graph with multiple children per parent

  // maximum path length for a graph with multiple children per parent
  private static final int HAS_SIBLINGS_MAXPATHLENGTH = 4;

  @Test
  public void noSiblingsPaths() {
    runComputePathsAndAssert(
        NO_SIBLINGS_ALLNODES,
        NO_SIBLINGS_PARENT_CHILD_RELATIONSHIPS,
        NO_SIBLINGS_EXPECTED_COMPUTE_PATHS,
        NO_SIBLINGS_MAXPATHLENGTH);
  }

  @Test
  public void hasSiblingsPaths() {
    runComputePathsAndAssert(
        HAS_SIBLINGS_ALLNODES,
        HAS_SIBLINGS_PARENT_CHILD_RELATIONSHIPS,
        HAS_SIBLINGS_EXPECTED_COMPUTE_PATHS,
        HAS_SIBLINGS_MAXPATHLENGTH);
  }

  /**
   * Run a test {@link PathUtils#computePaths} pipeline with the input nodes and parent-child
   * relationships. Assert that the expected paths are returned.
   */
  void runComputePathsAndAssert(
      List<Long> allNodes,
      List<Pair<Long, Long>> parentChildRelationships,
      List<Pair<Long, List<Long>>> expectedPaths,
      int maxPathLength) {

    List<Pair<Long, List<Long>>> nodePaths =
        PathUtils.computePaths(allNodes, parentChildRelationships, maxPathLength);

    assertThat(nodePaths, is(expectedPaths));
  }

  @Test
  public void noSiblingsNumChildren() {
    runCountChildrenAndAssert(
        NO_SIBLINGS_PARENT_CHILD_RELATIONSHIPS, NO_SIBLINGS_EXPECTED_NUM_CHILDREN);
  }

  @Test
  public void hasSiblingsNumChildren() {

    runCountChildrenAndAssert(
        HAS_SIBLINGS_PARENT_CHILD_RELATIONSHIPS, HAS_SIBLINGS_EXPECTED_NUM_CHILDREN);
  }

  /**
   * Run a test {@link PathUtils#countChildren} pipeline with the parent-child relationships. Assert
   * that the expected number of children are returned.
   */
  void runCountChildrenAndAssert(
      Collection<Pair<Long, Long>> parentChildRelationships, Map<Long, Long> expectedNumChildren) {

    Map<Long, Long> nodeNumChildren = PathUtils.countChildren(parentChildRelationships);
    assertThat(nodeNumChildren, is(expectedNumChildren));
  }

  @Test
  public void noSiblingsPrunedPaths() {
    List<Pair<Long, List<Long>>> expectedPrunedPaths =
        List.of(
            Pair.of(11L, List.of(21L, 31L)),
            Pair.of(21L, List.of(31L)),
            Pair.of(31L, List.of()),
            Pair.of(12L, List.of(22L)),
            Pair.of(22L, List.of()));

    runPruneOrphanPathsAndAssert(
        NO_SIBLINGS_EXPECTED_COMPUTE_PATHS, NO_SIBLINGS_EXPECTED_NUM_CHILDREN, expectedPrunedPaths);
  }

  @Test
  public void hasSiblingsPrunedPaths() {
    List<Pair<Long, List<Long>>> expectedPrunedPaths =
        List.of(
            Pair.of(10L, List.of(21L, 31L)),
            Pair.of(11L, List.of(21L, 31L)),
            Pair.of(20L, List.of(31L)),
            Pair.of(21L, List.of(31L)),
            Pair.of(31L, List.of()),
            Pair.of(12L, List.of(22L)),
            Pair.of(22L, List.of()));

    runPruneOrphanPathsAndAssert(
        HAS_SIBLINGS_EXPECTED_COMPUTE_PATHS,
        HAS_SIBLINGS_EXPECTED_NUM_CHILDREN,
        expectedPrunedPaths);
  }

  /**
   * Run a test {@link PathUtils#pruneOrphanPaths} pipeline with the input nodes and parent-child
   * relationships. Assert that the expected path and number of children are returned.
   */
  void runPruneOrphanPathsAndAssert(
      List<Pair<Long, List<Long>>> nodePaths,
      Map<Long, Long> nodeNumChildren,
      Collection<Pair<Long, List<Long>>> expectedPrunedPaths) {
    var nodePathAndNumChildren = PathUtils.pruneOrphanPaths(nodePaths, nodeNumChildren);

    assertThat(nodePathAndNumChildren, is(expectedPrunedPaths));
  }

  @Test
  public void filteredRootNodes() {
    List<Long> possibleRootNodes = List.of(10L, 20L, 31L);
    List<Pair<Long, List<Long>>> nodePaths =
        List.of(
            Pair.of(10L, List.of(21L, 31L)),
            Pair.of(11L, List.of(21L, 31L)),
            Pair.of(20L, List.of(31L)),
            Pair.of(21L, List.of(31L)),
            Pair.of(31L, List.of()),
            Pair.of(12L, List.of(22L)),
            Pair.of(22L, List.of()),
            Pair.of(13L, List.of()));

    List<Pair<Long, List<Long>>> expectedFilteredPaths =
        List.of(
            Pair.of(10L, List.of(21L, 31L)),
            Pair.of(11L, List.of(21L, 31L)),
            Pair.of(20L, List.of(31L)),
            Pair.of(21L, List.of(31L)),
            Pair.of(31L, List.of()),
            Pair.of(12L, List.of(22L)),
            Pair.of(22L, List.of()),
            Pair.of(13L, List.of()));

    runFilterRootNodesAndAssert(possibleRootNodes, nodePaths, expectedFilteredPaths);
  }

  /**
   * Run a test {@link PathUtils#filterRootNodes} pipeline with the possible root nodes and
   * node-path pairs. Assert that the expected paths are returned.
   */
  void runFilterRootNodesAndAssert(
      List<Long> possibleRootNodes,
      List<Pair<Long, List<Long>>> nodePaths,
      List<Pair<Long, List<Long>>> expectedFilteredNodePaths) {

    Collection<Pair<Long, List<Long>>> filteredNodePaths =
        PathUtils.filterRootNodes(possibleRootNodes, nodePaths);

    assertThat(filteredNodePaths, is(expectedFilteredNodePaths));
  }
}
