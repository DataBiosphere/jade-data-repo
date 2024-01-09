package bio.terra.tanagra.indexing.job.beam;

import java.util.Iterator;
import java.util.List;
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

@edu.umd.cs.findbugs.annotations.SuppressFBWarnings(
    value = {"NP_NULL_PARAM_DEREF", "RCN_REDUNDANT_NULLCHECK_OF_NONNULL_VALUE"},
    justification = "PCollection is using a Nullable coder")
public final class PathUtils {
  private PathUtils() {}

  // the path of nodes is currently encoded as a string. use this as the delimiter between nodes.
  private static final String PATH_DELIMITER = ".";
  private static final String PATH_DELIMITER_REGEX = "\\.";

  /**
   * Compute one path from each node in a hierarchy to a root node. If there are multiple paths from
   * a given node to a root node, then select one at random.
   *
   * @param allNodes a collection of all nodes in the hierarchy
   * @param childParentRelationships a collection of all child-parent relationships in the hierarchy
   * @param maxPathLengthHint the maximum path length to handle
   * @return a collection of (node, path) mappings
   */
  public static PCollection<KV<Long, String>> computePaths(
      PCollection<Long> allNodes,
      PCollection<KV<Long, Long>> childParentRelationships,
      int maxPathLengthHint) {
    // build a collection of KV<node,path> where path="initial node"
    PCollection<KV<Long, String>> nextNodePathKVs =
        allNodes.apply(
            "build (node,path) KV pairs",
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.longs(), TypeDescriptors.strings()))
                .via(node -> KV.of(node, node.toString())));

    // define the CoGroupByKey tags
    final TupleTag<String> pathTag = new TupleTag<>();
    final TupleTag<Long> parentTag = new TupleTag<>();

    /* Example for CoGroupByKey+ParDo iteration below:
    desired result (node,path)
    (a1,"a1.a2.a3")
    (a2,"a2.a3")
    (a3,"a3")
    (b1,"b1.b2")
    (b2,"b2")
    (c1,"c1")

    childParentKVs (child,parent)
    (a1,"a2")
    (a2,"a3")
    (b1,"b2")

    [iteration 0] nodePathKVs (next node,path)
    (a1,"a1")
    (a2,"a2")
    (a3,"a3")
    (b1,"b1")
    (b2,"b2")
    (c1,"c1")
    [iteration 0] nodeParentPathJoin (next node,paths,next node parents)
    (a1,["a1"],["a2"])
    (a2,["a2"],["a3"])
    (a3,["a3"],[])
    (b1,["b1"],["b2"])
    (b2,["b2"],[])
    (c1,["c1"],[])

    [iteration 1] nodePathKVs (next node,path)
    (a2,"a2.a1")
    (a3,"a3.a2")
    (a3,"a3")
    (b2,"b2.b1")
    (b2,"b2")
    (c1,"c1")
    [iteration 1] nodeParentPathJoin (next node,paths,next node parents)
    (a1,[],["a2"])
    (a2,["a2.a1"],["a3"])
    (a3,["a3.a2","a3"],[])
    (b1,[],["b2]")
    (b2,["b2.b1","b2"],[])
    (c1,["c1"],[]) */
    // iterate through each possible level of the hierarchy, adding up to one node to each path per
    // iteration
    for (int ctr = 0; ctr < maxPathLengthHint; ctr++) {
      // do a CoGroupByKey join of the current node-path collection and the child-parent collection
      PCollection<KV<Long, CoGbkResult>> nodeParentPathJoin =
          KeyedPCollectionTuple.of(pathTag, nextNodePathKVs)
              .and(parentTag, childParentRelationships)
              .apply("join node-path and child-parent collections: " + ctr, CoGroupByKey.create());

      // run a ParDo for each row of the join result
      nextNodePathKVs =
          nodeParentPathJoin.apply(
              "run ParDo for each row of the node-path and child-parent join result: " + ctr,
              ParDo.of(
                  new DoFn<KV<Long, CoGbkResult>, KV<Long, String>>() {
                    @ProcessElement
                    public void processElement(ProcessContext context) {
                      KV<Long, CoGbkResult> element = context.element();
                      Long node = element.getKey();
                      Iterator<String> pathTagIter = element.getValue().getAll(pathTag).iterator();
                      Iterator<Long> parentTagIter =
                          element.getValue().getAll(parentTag).iterator();

                      // there may be multiple possible next steps (i.e. the current node may have
                      // multiple parents). just pick the first one
                      Long nextNodeInPath = parentTagIter.hasNext() ? parentTagIter.next() : null;

                      // iterate through all the paths that need this relationship to complete their
                      // next step
                      while (pathTagIter.hasNext()) {
                        String currentPath = pathTagIter.next();

                        if (nextNodeInPath == null) {
                          // if there are no relationships to complete the next step, then we've
                          // reached a root node. just keep the path as is
                          context.output(KV.of(node, currentPath));
                        } else {
                          // if there is a next node, then append it to the path and make it the new
                          // key (i.e. the new next node)
                          context.output(
                              KV.of(nextNodeInPath, currentPath + PATH_DELIMITER + nextNodeInPath));
                        }
                      }
                    }
                  }));
    }

    // swap the key of all pairs from the next node in the path (which should all be root nodes at
    // this point), to the first node in the path. also trim the first node from the path.
    return nextNodePathKVs.apply(
        ParDo.of(
            new DoFn<KV<Long, String>, KV<Long, String>>() {
              @ProcessElement
              public void processElement(ProcessContext context) {
                KV<Long, String> kvPair = context.element();

                String path = kvPair.getValue();

                // strip out the first node in the path, and make it the key
                // e.g. (11,"11.21.31") => (11,"21.31")
                //      (12,"12") => (12,"")
                List<String> nodesInPath =
                    new java.util.ArrayList<>(List.of(path.split(PATH_DELIMITER_REGEX)));
                String firstNodeInPath = nodesInPath.remove(0);
                String pathWithoutFirstNode =
                    nodesInPath.stream().collect(Collectors.joining(PATH_DELIMITER));

                Long firstNode = Long.valueOf(firstNodeInPath);

                context.output(KV.of(firstNode, pathWithoutFirstNode));
              }
            }));
  }

  /**
   * Count the number of children that each node has.
   *
   * @param allNodes a collection of all nodes in the hierarchy
   * @param childParentRelationships a collection of all child-parent relationships in the hierarchy
   * @return a collection of (node, numChildren) mappings
   */
  public static PCollection<KV<Long, Long>> countChildren(
      PCollection<Long> allNodes, PCollection<KV<Long, Long>> childParentRelationships) {
    // build a collection of KV<node,numChildren> where numChildren=0
    PCollection<KV<Long, Long>> nextNodePathKVs =
        allNodes.apply(
            "build (node,numChildren) KV pairs",
            MapElements.into(TypeDescriptors.kvs(TypeDescriptors.longs(), TypeDescriptors.longs()))
                .via(node -> KV.of(node, 0L)));

    // reverse the key and value of the child-parent KVs => parent-child KVs
    PCollection<KV<Long, Long>> parentChildKVs =
        childParentRelationships.apply(
            "reverse order of child-parent to parent-child KVs", KvSwap.create());

    // define the CoGroupByKey tags
    final TupleTag<Long> numChildrenTag = new TupleTag<>();
    final TupleTag<Long> childTag = new TupleTag<>();

    // do a CoGroupByKey join of the current node-numChildren collection and the parent-child
    // collection
    PCollection<KV<Long, CoGbkResult>> nodeNumChildrenJoin =
        KeyedPCollectionTuple.of(numChildrenTag, nextNodePathKVs)
            .and(childTag, parentChildKVs)
            .apply("join node-numChildren and parent-child collections", CoGroupByKey.create());

    // run a ParDo for each row of the join result
    return nodeNumChildrenJoin.apply(
        "run ParDo for each row of the node-numChildren and parent-child join result",
        ParDo.of(
            new DoFn<KV<Long, CoGbkResult>, KV<Long, Long>>() {
              @ProcessElement
              public void processElement(ProcessContext context) {
                KV<Long, CoGbkResult> element = context.element();
                Long node = element.getKey();
                Iterator<Long> numChildrenTagIter =
                    element.getValue().getAll(numChildrenTag).iterator();
                Iterator<Long> childTagIter = element.getValue().getAll(childTag).iterator();

                // if the parent node in the relationship is not a member of all the nodes, then
                // skip processing
                if (!numChildrenTagIter.hasNext()) {
                  return;
                }

                // count the number of children
                long numChildren = numChildrenTagIter.next();
                while (childTagIter.hasNext()) {
                  childTagIter.next();
                  numChildren++;
                }

                context.output(KV.of(node, numChildren));
              }
            }));
  }

  /**
   * Prune orphan nodes from the hierarchy (i.e. set path=null for nodes with no parents or
   * children).
   *
   * @param nodePaths a collection of all (node, path) mappings, where every node has a non-null
   *     path (i.e. orphan nodes are root nodes)
   * @param nodeNumChildren a collection of all (node, numChildren) mappings,
   * @return a collection of (node, path) mappings, where orphan nodes have their path=null
   */
  public static PCollection<KV<Long, String>> pruneOrphanPaths(
      PCollection<KV<Long, String>> nodePaths, PCollection<KV<Long, Long>> nodeNumChildren) {
    // define the CoGroupByKey tags
    final TupleTag<String> pathTag = new TupleTag<>();
    final TupleTag<Long> numChildrenTag = new TupleTag<>();

    // do a CoGroupByKey join of the current node-numChildren collection and the parent-child
    // collection
    PCollection<KV<Long, CoGbkResult>> pathNumChildrenJoin =
        KeyedPCollectionTuple.of(pathTag, nodePaths)
            .and(numChildrenTag, nodeNumChildren)
            .apply("join node-path and node-numChildren collections", CoGroupByKey.create());

    // run a ParDo for each row of the join result
    PCollection<KV<Long, String>> prunedPaths =
        pathNumChildrenJoin.apply(
            "run ParDo for each row of the node-path and node-numChildren join result",
            ParDo.of(
                new DoFn<KV<Long, CoGbkResult>, KV<Long, String>>() {
                  @ProcessElement
                  public void processElement(ProcessContext context) {
                    KV<Long, CoGbkResult> element = context.element();
                    Long node = element.getKey();
                    Iterator<String> pathTagIter = element.getValue().getAll(pathTag).iterator();
                    Iterator<Long> numChildrenTagIter =
                        element.getValue().getAll(numChildrenTag).iterator();

                    String path = pathTagIter.next();
                    Long numChildren = numChildrenTagIter.next();

                    if (path.isEmpty() && numChildren == 0) {
                      path = null;
                    }
                    context.output(KV.of(node, path));
                  }
                }));
    prunedPaths.setCoder(KvCoder.of(VarLongCoder.of(), NullableCoder.of(StringUtf8Coder.of())));
    return prunedPaths;
  }

  /**
   * Filter the root nodes in the hierarchy (i.e. set path=null for nodes that currently have
   * path="" but are not members of the possible root nodes.
   *
   * @param possibleRootNodes a collection of all possible root nodes
   * @param nodePaths a collection of all (node, path) mappings, where nodes may be null
   * @return a collection of (node, path) mappings, where root nodes that are not one of the
   *     possible root nodes have their path updated to null
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
}
