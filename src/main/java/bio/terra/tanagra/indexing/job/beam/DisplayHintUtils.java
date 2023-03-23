package bio.terra.tanagra.indexing.job.beam;

import java.io.Serializable;
import org.apache.beam.sdk.transforms.Count;
import org.apache.beam.sdk.transforms.Distinct;
import org.apache.beam.sdk.transforms.Filter;
import org.apache.beam.sdk.transforms.MapElements;
import org.apache.beam.sdk.transforms.Max;
import org.apache.beam.sdk.transforms.Min;
import org.apache.beam.sdk.transforms.join.CoGbkResult;
import org.apache.beam.sdk.transforms.join.CoGroupByKey;
import org.apache.beam.sdk.transforms.join.KeyedPCollectionTuple;
import org.apache.beam.sdk.values.KV;
import org.apache.beam.sdk.values.PCollection;
import org.apache.beam.sdk.values.TupleTag;
import org.apache.beam.sdk.values.TypeDescriptor;
import org.apache.beam.sdk.values.TypeDescriptors;

public final class DisplayHintUtils<T> {
  private DisplayHintUtils() {}

  public static PCollection<IdNumericRange> numericRangeHint(
      PCollection<KV<Long, Double>> criteriaValuePairs) {
    // Combine-min by key: [key] criteriaId, [value] min attr value
    PCollection<KV<Long, Double>> criteriaMinPairs = criteriaValuePairs.apply(Min.doublesPerKey());

    // Combine-max by key: [key] criteriaId, [value] max attr value
    PCollection<KV<Long, Double>> criteriaMaxPairs = criteriaValuePairs.apply(Max.doublesPerKey());

    // Group by [key] criteriaId: [values] min, max
    final TupleTag<Double> minTag = new TupleTag<>();
    final TupleTag<Double> maxTag = new TupleTag<>();
    PCollection<KV<Long, CoGbkResult>> criteriaAndMinMax =
        KeyedPCollectionTuple.of(minTag, criteriaMinPairs)
            .and(maxTag, criteriaMaxPairs)
            .apply(CoGroupByKey.create());

    // Build NumericRange object per criteriaId to return.
    return criteriaAndMinMax.apply(
        MapElements.into(TypeDescriptor.of(IdNumericRange.class))
            .via(
                cogb -> {
                  Long criteriaId = cogb.getKey();
                  Double min = cogb.getValue().getOnly(minTag);
                  Double max = cogb.getValue().getOnly(maxTag);
                  return new IdNumericRange(criteriaId, min, max);
                }));
  }

  public static PCollection<IdEnumValue> enumValHint(
      PCollection<KV<IdEnumValue, Long>> criteriaEnumPrimaryPairs) {
    // Build key-value pairs for criteriaId+enum value/display.
    // [key] serialized format (by built-in functions), [value] deserialized object
    // [key] "criteriaId-value-display", [value] criteria+enum value/display
    PCollection<KV<String, IdEnumValue>> serializedAndDeserialized =
        criteriaEnumPrimaryPairs.apply(
            MapElements.into(
                    TypeDescriptors.kvs(
                        TypeDescriptors.strings(), new TypeDescriptor<IdEnumValue>() {}))
                .via(
                    enumValueInstancePrimaryPair ->
                        KV.of(
                            enumValueInstancePrimaryPair.getKey().toString(),
                            enumValueInstancePrimaryPair.getKey())));

    // Build key-value pairs: [key] serialized criteriaId+enum value/display, [value] primaryId.
    PCollection<KV<String, Long>> serializedPrimaryPairs =
        criteriaEnumPrimaryPairs.apply(
            MapElements.into(
                    TypeDescriptors.kvs(TypeDescriptors.strings(), TypeDescriptors.longs()))
                .via(
                    enumValueInstancePrimaryPair ->
                        KV.of(
                            enumValueInstancePrimaryPair.getKey().toString(),
                            enumValueInstancePrimaryPair.getValue())));

    // Distinct (remove duplicate persons).
    // [key] serialized criteriaId+enum value/display, [value] primaryId
    PCollection<KV<String, Long>> distinctSerializedPrimaryPairs =
        serializedPrimaryPairs.apply(Distinct.create());

    // Count by key: [key] serialized criteriaId+enum value/display, [value] num primaryId.
    PCollection<KV<String, Long>> serializedCountPairs =
        distinctSerializedPrimaryPairs.apply(Count.perKey());

    // Group by [key] serialized criteriaId+enum value/display
    // [values] deserialized object, num primaryId.
    final TupleTag<IdEnumValue> deserializedTag = new TupleTag<>();
    final TupleTag<Long> numPrimaryIdTag = new TupleTag<>();
    PCollection<KV<String, CoGbkResult>> serializedAndDeserializedNumPrimaryId =
        KeyedPCollectionTuple.of(deserializedTag, serializedAndDeserialized)
            .and(numPrimaryIdTag, serializedCountPairs)
            .apply(CoGroupByKey.create());
    return serializedAndDeserializedNumPrimaryId
        .apply(
            Filter.by(
                cogb ->
                    cogb.getValue() != null
                        && cogb.getValue().getAll(deserializedTag).iterator().hasNext()))
        .apply(
            MapElements.into(new TypeDescriptor<IdEnumValue>() {})
                .via(
                    cogb -> {
                      IdEnumValue deserialized =
                          cogb.getValue().getAll(deserializedTag).iterator().next();
                      Long count = cogb.getValue().getOnly(numPrimaryIdTag);
                      return new IdEnumValue(
                              deserialized.getId(),
                              deserialized.getEnumValue(),
                              deserialized.getEnumDisplay())
                          .count(count);
                    }));
  }

  public static class IdNumericRange implements Serializable {
    private final Long id;
    private final double min;
    private final double max;

    public IdNumericRange(Long id, double min, double max) {
      this.id = id;
      this.min = min;
      this.max = max;
    }

    public Long getId() {
      return id;
    }

    public double getMax() {
      return max;
    }

    public double getMin() {
      return min;
    }
  }

  public static class IdEnumValue implements Serializable {
    private final Long id;
    // TODO: Parameterize this class based on the type of the enum value. I couldn't get Beam to
    // recognize the coder for this class when it's a generic type.
    private final String enumValue;
    private final String enumDisplay;
    private long count;

    public IdEnumValue(Long id, String enumValue, String enumDisplay) {
      this.id = id;
      this.enumValue = enumValue;
      this.enumDisplay = enumDisplay;
    }

    public Long getId() {
      return id;
    }

    public String getEnumValue() {
      return enumValue;
    }

    public String getEnumDisplay() {
      return enumDisplay;
    }

    public long getCount() {
      return count;
    }

    public IdEnumValue count(long count) {
      this.count = count;
      return this;
    }

    @Override
    public String toString() {
      return id + " - " + (enumValue == null ? "null" : enumValue) + " - " + enumDisplay;
    }
  }
}
