package bio.terra.tanagra.indexing.job.beam;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.beam.sdk.coders.KvCoder;
import org.apache.beam.sdk.coders.NullableCoder;
import org.apache.beam.sdk.coders.StringUtf8Coder;
import org.apache.beam.sdk.coders.VarLongCoder;
import org.apache.beam.sdk.transforms.DoFn;
import org.apache.beam.sdk.transforms.KvSwap;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.ParDo;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptors;
import org.apache.commons.lang3.tuple.Pair;

@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
    value = {"NP_NULL_PARAM_DEREF", "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE"},
    justification = "PCollection is using a Nullable coder")
public final class PathUtils {
  private PathUtils() {
  }

  // the path of nodes is currently encoded as a string. use this as the delimiter between nodes.
  private static final String PATH_DELIMITER = ".";
  private static final String PATH_DELIMITER_REGEX = "\\.";

  /**
   * Compute one path from each node in a hierarchy to a root node. If there are multiple paths from
   * a given node to a root node, then select one at random.
   *
   * @param allNodes                 a collection of all nodes in the hierarchy
   * @param childParentRelationships a collection of all child-parent relationships in the
   *                                 hierarchy
   * @param maxPathLengthHint        the maximum path length to handle
   * @return a collection of (node, path) mappings
   */
  public static List<Pair<Long, List<Long>>> computePaths(
      List<Long> allNodes, List<Pair<Long, Long>> childParentRelationships, int maxPathLengthHint) {

    /*
    desired result (node,path)
    (a1, [a2, a3])
    (a2, [a3])
    (a3, [])
    (b1, [b2])
    (b2, [])
    (c1, [])

    childParentRelationships (child,parent)
    (a1,"a2")
    (a2,"a3")
    (b1,"b2")
     */
    /*
     * Given a list of child parent relationships, generate a map of child to all its parents.
     */
    List<Pair<Long, List<Long>>> nodePaths =
        allNodes.stream().map(node -> Pair.<Long, List<Long>>of(node, new ArrayList<Long>())).toList();

    for (int ctr = 0; ctr < maxPathLengthHint; ctr++) {
      for (var nodePath : nodePaths) {
        List<Long> path = nodePath.getValue();
        childParentRelationships.stream()
            .filter(relationship -> relationship.getKey() == (path.isEmpty() ? nodePath.getKey() : path.get(path.size() - 1)))
            // A child may have multiple parents. Just pick the first one.
            .findFirst()
            .ifPresent(relationship -> path.add(relationship.getValue()));
      }
    }

    return nodePaths;
  }

  /**
   * Count the number of children that each node has.
   *
   * @param childParentRelationships a collection of all child-parent relationships in the
   *                                 hierarchy
   * @return a collection of (node, numChildren) mappings
   */
  public static Map<Long, Long> countChildren(Collection<Pair<Long, Long>> childParentRelationships) {
    // For each parent, how many children does it have?
    return childParentRelationships.stream()
            .collect(Collectors.groupingBy(Pair::getValue, Collectors.counting()));
  }

  /**
   * Prune orphan nodes from the hierarchy (i.e. set path=null for nodes with no parents or
   * children).
   *
   * @param nodePaths       a collection of all (node, path) mappings, where every node has a
   *                        non-null path (i.e. orphan nodes are root nodes)
   * @param nodeNumChildren a collection of all (node, numChildren) mappings,
   * @return a collection of (node, path) mappings, where orphan nodes have their path=null
   */
  public static Collection<Pair<Long, List<Long>>> pruneOrphanPaths(
      Collection<Pair<Long, List<Long>>> nodePaths, Map<Long, Long> nodeNumChildren) {
    return nodePaths.stream()
        .filter(
            nodePath ->
                nodeNumChildren.getOrDefault(nodePath.getKey(), 0L) > 0
                    || !nodePath.getValue().isEmpty())
        .toList();
  }

  /**
   * Filter the root nodes in the hierarchy (i.e. set path=null for nodes that currently have
   * path="" but are not members of the possible root nodes.
   *
   * @param possibleRootNodes a collection of all possible root nodes
   * @param nodePaths         a collection of all (node, path) mappings, where nodes may be null
   * @return a collection of (node, path) mappings, where root nodes that are not one of the
   * possible root nodes have their path updated to null
   */
  public static PCollection<KV<Long, String>> filterRootNodes(
      PCollection<Long> possibleRootNodes, PCollection<KV<Long, String>> nodePaths) {
    // build a collection of KV<rootNode, fullPath>
    PCollection<KV<Long, String>> rootNodeFullPathKVs =
        nodePaths.apply(
            "build (rootNode,fullPath) KV pairs",
            ParDo.of(
                new DoFn<KV<Long, String>, KV<Long, String>>() {
                  @ProcessElement
                  public void processElement(ProcessContext context) {
                    KV<Long, String> element = context.element();
                    Long firstNodeInPath = element.getKey();
                    String pathWithoutFirstNode = element.getValue();

                    if (pathWithoutFirstNode == null) {
                      // if this node is not part of the hierarchy,
                      // then there's nothing to update. leave the pair unchanged
                      // e.g. (12,null) => (12,null)
                      context.output(KV.of(firstNodeInPath, pathWithoutFirstNode));
                    } else if (pathWithoutFirstNode.isEmpty()) {
                      // if this node is a root node, then add the root node to the path.
                      // e.g. (31,"") => (31,"31")
                      context.output(KV.of(firstNodeInPath, firstNodeInPath.toString()));
                    } else {
                      // strip out the root node from the path, and make it the key
                      // append the first node in the path to the beginning of the path, and make it
                      // the value
                      // e.g. (11,"21.31") => (31,"11.21.31")
                      List<String> nodesInPath =
                          new java.util.ArrayList<>(
                              List.of(pathWithoutFirstNode.split(PATH_DELIMITER_REGEX)));
                      Long rootNode = Long.valueOf(nodesInPath.get(nodesInPath.size() - 1));
                      nodesInPath.add(0, firstNodeInPath.toString());
                      String pathWithFirstNode =
                          nodesInPath.stream().collect(Collectors.joining(PATH_DELIMITER));

                      context.output(KV.of(rootNode, pathWithFirstNode));
                    }
                  }
                }));

    // build a collection of KV<node,possibleRoot> where possibleRoot=1
    PCollection<KV<Long, Long>> possibleRootNodeKVs =
        possibleRootNodes.apply(
            "build (node,possibleRoot) KV pairs",
            MapElements.into(TypeDescriptors.kvs(TypeDescriptors.longs(), TypeDescriptors.longs()))
                .via(node -> KV.of(node, 1L)));

    // define the CoGroupByKey tags
    final TupleTag<String> pathTag = new TupleTag<>();
    final TupleTag<Long> possibleRootTag = new TupleTag<>();

    // do a CoGroupByKey join of the rootNode-fullPath collection and the node-possibleRoot
    // collection
    PCollection<KV<Long, CoGbkResult>> pathPossibleRootJoin =
        KeyedPCollectionTuple.of(pathTag, rootNodeFullPathKVs)
            .and(possibleRootTag, possibleRootNodeKVs)
            .apply(
                "join rootNode-fullPath and node-possibleRoot collections", CoGroupByKey.create());

    // run a ParDo for each row of the join result
    PCollection<KV<Long, String>> filteredPaths =
        pathPossibleRootJoin.apply(
            "run ParDo for each row of the rootNode-fullPath and node-possibleRoot join result",
            ParDo.of(
                new DoFn<KV<Long, CoGbkResult>, KV<Long, String>>() {
                  @ProcessElement
                  public void processElement(ProcessContext context) {
                    KV<Long, CoGbkResult> element = context.element();
                    Long node = element.getKey();
                    Iterator<String> pathTagIter = element.getValue().getAll(pathTag).iterator();
                    Iterator<Long> possibleRootTagIter =
                        element.getValue().getAll(possibleRootTag).iterator();

                    boolean isPossibleRoot = possibleRootTagIter.hasNext();
                    while (pathTagIter.hasNext()) {
                      String fullPath = pathTagIter.next();

                      if (fullPath == null) {
                        // if this node is not part of the hierarchy, then there's nothing to
                        // update.
                        context.output(KV.of(node, null));
                      } else {
                        // strip out the first node in the path
                        // e.g. (31,"11.21.31") => (11,"21.31")
                        List<String> nodesInPath =
                            new java.util.ArrayList<>(
                                List.of(fullPath.split(PATH_DELIMITER_REGEX)));
                        Long firstNodeInPath = Long.valueOf(nodesInPath.remove(0));
                        String path =
                            isPossibleRoot ? String.join(PATH_DELIMITER, nodesInPath) : null;

                        context.output(KV.of(firstNodeInPath, path));
                      }
                    }
                  }
                }));
    filteredPaths.setCoder(KvCoder.of(VarLongCoder.of(), NullableCoder.of(StringUtf8Coder.of())));
    return filteredPaths;
  }

  public static Collection<Pair<Long, List<Long>>> filterRootNodes(
      Collection<Long> possibleRootNodes, Collection<Pair<Long, List<Long>>> nodePaths) {
    // build a collection of KV<rootNode, fullPath>
    Collection<Pair<Long, String>> rootNodeFullPathKVs =
        nodePaths.stream().map(element -> {
long firstNodeInPath = element.getKey();
List<Long> pathWithoutFirstNode = element.getValue();
if (pathWithoutFirstNode.isEmpty()) {
  return Pair.of(firstNodeInPath, new ArrayList<>(List.of(firstNodeInPath)));
} else {
            }
            ).toList();
    /*
        nodePaths.apply(
            "build (rootNode,fullPath) KV pairs",
            ParDo.of(
                new DoFn<KV<Long, String>, KV<Long, String>>() {
                  @ProcessElement
                  public void processElement(ProcessContext context) {
                    KV<Long, String> element = context.element();
                    Long firstNodeInPath = element.getKey();
                    String pathWithoutFirstNode = element.getValue();

                    if (pathWithoutFirstNode == null) {
                      // if this node is not part of the hierarchy,
                      // then there's nothing to update. leave the pair unchanged
                      // e.g. (12,null) => (12,null)
                      context.output(KV.of(firstNodeInPath, null));
                    } else if (pathWithoutFirstNode.isEmpty()) {
                      // if this node is a root node, then add the root node to the path.
                      // e.g. (31,"") => (31,"31")
                      context.output(KV.of(firstNodeInPath, firstNodeInPath.toString()));
                    } else {
                      // strip out the root node from the path, and make it the key
                      // append the first node in the path to the beginning of the path, and make it
                      // the value
                      // e.g. (11,"21.31") => (31,"11.21.31")
                      List<String> nodesInPath =
                          new java.util.ArrayList<>(
                              List.of(pathWithoutFirstNode.split(PATH_DELIMITER_REGEX)));
                      Long rootNode = Long.valueOf(nodesInPath.get(nodesInPath.size() - 1));
                      nodesInPath.add(0, firstNodeInPath.toString());
                      String pathWithFirstNode =
                          nodesInPath.stream().collect(Collectors.joining(PATH_DELIMITER));

                      context.output(KV.of(rootNode, pathWithFirstNode));
                    }
                  }
                }));

    // build a collection of KV<node,possibleRoot> where possibleRoot=1
    PCollection<KV<Long, Long>> possibleRootNodeKVs =
        possibleRootNodes.apply(
            "build (node,possibleRoot) KV pairs",
            MapElements.into(TypeDescriptors.kvs(TypeDescriptors.longs(), TypeDescriptors.longs()))
                .via(node -> KV.of(node, 1L)));

    // define the CoGroupByKey tags
    final TupleTag<String> pathTag = new TupleTag<>();
    final TupleTag<Long> possibleRootTag = new TupleTag<>();

    // do a CoGroupByKey join of the rootNode-fullPath collection and the node-possibleRoot
    // collection
    PCollection<KV<Long, CoGbkResult>> pathPossibleRootJoin =
        KeyedPCollectionTuple.of(pathTag, rootNodeFullPathKVs)
            .and(possibleRootTag, possibleRootNodeKVs)
            .apply(
                "join rootNode-fullPath and node-possibleRoot collections", CoGroupByKey.create());

    // run a ParDo for each row of the join result
    PCollection<KV<Long, String>> filteredPaths =
        pathPossibleRootJoin.apply(
            "run ParDo for each row of the rootNode-fullPath and node-possibleRoot join result",
            ParDo.of(
                new DoFn<KV<Long, CoGbkResult>, KV<Long, String>>() {
                  @ProcessElement
                  public void processElement(ProcessContext context) {
                    KV<Long, CoGbkResult> element = context.element();
                    Long node = element.getKey();
                    Iterator<String> pathTagIter = element.getValue().getAll(pathTag).iterator();
                    Iterator<Long> possibleRootTagIter =
                        element.getValue().getAll(possibleRootTag).iterator();

                    boolean isPossibleRoot = possibleRootTagIter.hasNext();
                    while (pathTagIter.hasNext()) {
                      String fullPath = pathTagIter.next();

                      if (fullPath == null) {
                        // if this node is not part of the hierarchy, then there's nothing to
                        // update.
                        context.output(KV.of(node, null));
                      } else {
                        // strip out the first node in the path
                        // e.g. (31,"11.21.31") => (11,"21.31")
                        List<String> nodesInPath =
                            new java.util.ArrayList<>(
                                List.of(fullPath.split(PATH_DELIMITER_REGEX)));
                        Long firstNodeInPath = Long.valueOf(nodesInPath.remove(0));
                        String path =
                            isPossibleRoot ? String.join(PATH_DELIMITER, nodesInPath) : null;

                        context.output(KV.of(firstNodeInPath, path));
                      }
                    }
                  }
                }));
    filteredPaths.setCoder(KvCoder.of(VarLongCoder.of(), NullableCoder.of(StringUtf8Coder.of())));
    return filteredPaths;
  }
}
