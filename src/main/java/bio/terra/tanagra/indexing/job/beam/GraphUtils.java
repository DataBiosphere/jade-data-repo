package bio.terra.tanagra.indexing.job.beam;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.Flatten;
import org.apache.beam.sdk.transforms.KvSwap;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.PCollectionList;
import org.apache.beam.sdk.values.TupleTag;

/** Utilities for working with graphs in Apache Beam. */
public final class GraphUtils {
  private GraphUtils() {}

  /**
   * Computes the transitive closure of a directed graph defined by {@code edges}.
   *
   * <p>The returned PCollection has at least one KV for every pair of vertices [X, Y] where X can
   * reach Y by traversing the input edges. e.g. If the edges are [{A, B}, {B, C}], then the output
   * will be [{A, B}, {A, C}, {B, C}].
   *
   * @param edges A directed acyclic graph where every KV defines a directed edge with a {Head,
   *     Tail} vertex pair.
   * @param maxPathLengthHint The maximum path length to try to find in the graph. If there is a
   *     path in the graph longer than this, not all paths will be found. This is used to bound
   *     execution.
   * @param <T> The type of the vertex in the graph.
   * @return A PCollection of all reachable vertex pairs, where there may be duplicate pairs.
   */
  public static <T> PCollection<KV<T, T>> transitiveClosure(
      PCollection<KV<T, T>> edges, int maxPathLengthHint) {
    // allPaths[n] is the set of vertices {V0, V1}, where V0 -> V1 is reachable in N or less edges.
    // That is, there is a path with N or less edges from V0 to V1.
    Map<Integer, PCollection<KV<T, T>>> allPaths = new HashMap<>();
    // exactPaths[n] is the set of vertices where there exists a path from V0 -> V1 in exactly N
    // edges.
    Map<Integer, PCollection<KV<T, T>>> exactPaths = new HashMap<>();

    // The input edges are both allPaths[1] and exactPaths[1].
    allPaths.put(1, edges);
    exactPaths.put(1, edges);

    // Iteratively build up allPaths in O(log_2(maxPathLengthHint)) joins. We want to avoid doing
    // many joins or creating many duplicate paths that will increase the runtime.
    // The simplest algorithm would be to compute all paths incrementally one edge length at a time,
    // taking O(maxPathLengthHint) joins:
    // allPaths[x+1] = allPaths[x] + exactPaths[x] * allPaths[1],
    // where '+' is appending lists, and '*' is concatenating paths together, joining on vertexes
    // e.g. [{A, B}] * [{B, C}, {B, D}, {D, E}] yields [{A, C}, {A,D}].
    //
    // Given that exactPaths[x + y] = exactPaths[x] * exactPaths[y], and
    // and that for x >= y, allPaths[x] = allPaths[y - 1] + exactPaths[y] * allPaths[x - y], we have
    // allPaths[2n - 1] = allPaths[n - 1] + exactPaths[n] + exactPaths[n] * allPaths[n - 1].
    //
    // With this, we can increase n exponentially by 2, allowing for O(log_2(maxPathLengths) joins,
    // (n=2) allPaths[3] = allPaths[1] + exactPaths[2] + exactPaths[2] * allPaths[1]
    // (n=4) allPaths[7] = allPaths[3] + exactPaths[4] + exactPaths[4] * allPaths[3]
    // etc. where exactPaths[n] = exactPaths[n/2] * exactPaths[n/2].
    // See also
    // https://asingleneuron.files.wordpress.com/2013/10/distributedalgorithmsfortransitiveclosure.pdf
    int n = 2;
    while (true) {
      PCollection<KV<T, T>> nExactPaths =
          concatenate(
                  exactPaths.get(n / 2).apply(Distinct.create()),
                  exactPaths.get(n / 2).apply(Distinct.create()),
                  "exactPaths N" + n)
              .apply(Distinct.create());
      exactPaths.put(n, nExactPaths);

      PCollection<KV<T, T>> nMinus1AllPaths = allPaths.get(n - 1).apply(Distinct.create());

      PCollection<KV<T, T>> newAllPaths =
          PCollectionList.of(nMinus1AllPaths)
              .and(nExactPaths)
              .and(concatenate(nExactPaths, nMinus1AllPaths, "allPaths temp N" + n))
              .apply("flatten N" + n, Flatten.pCollections());
      int newLongestPathLength = 2 * n - 1;
      if (newLongestPathLength >= maxPathLengthHint) {
        // Stop when we have computes at least maxPathLengthHint length paths.
        // Apache Beam today does not support conditional iteration, so we cannot halt as soon as
        // there are no new paths being added. See https://issues.apache.org/jira/browse/BEAM-106
        return newAllPaths;
      }
      allPaths.put(newLongestPathLength, newAllPaths);
      n = n * 2;
    }
  }

  /**
   * Concatenate two collections of edges by joining their "middle" shared vertex.
   *
   * <p>E.g. if paths1 has {A, B} and paths2 has {B, C}, we output {A, C} by joining on B.
   *
   * <p>This only checks paths1 tail to paths2 head, but not paths2 tail to paths1 head. In the
   * current usage, this is sufficient and saves us some duplicates, but it is not technically a
   * full concatenation.
   */
  private static <T> PCollection<KV<T, T>> concatenate(
      PCollection<KV<T, T>> paths1, PCollection<KV<T, T>> paths2, String nameSuffix) {
    final TupleTag<T> t1 = new TupleTag<>();
    final TupleTag<T> t2 = new TupleTag<>();

    // Swap the key-values so that the middle vertex is the first key for both collections.
    PCollection<KV<T, T>> swappedPaths1 =
        paths1.apply("concatenateSwap " + nameSuffix, KvSwap.create());

    return KeyedPCollectionTuple.of(t1, swappedPaths1)
        .and(t2, paths2)
        .apply("concatenateJoin " + nameSuffix, CoGroupByKey.create())
        .apply(
            "concatenateTransform " + nameSuffix,
            ParDo.of(
                new DoFn<KV<T, CoGbkResult>, KV<T, T>>() {
                  @ProcessElement
                  public void processElement(ProcessContext context) {
                    KV<T, CoGbkResult> element = context.element();
                    // heads of paths1.
                    ImmutableList<T> heads = ImmutableList.copyOf(element.getValue().getAll(t1));
                    // tails of paths2.
                    ImmutableList<T> tails = ImmutableList.copyOf(element.getValue().getAll(t2));
                    // All combinations of heads and tails, where the 0th element is the head and
                    // the 1st element is the tail.
                    List<List<T>> newEdges = Lists.cartesianProduct(heads, tails);
                    newEdges.stream()
                        .map((List<T> edge) -> KV.of(edge.get(0), edge.get(1)))
                        .forEach(context::output);
                  }
                }));
  }
}
