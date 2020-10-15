package bio.terra.service.filedata;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.value.AutoValue;

import java.util.List;

/** Example AutoValue class with Jackson serialization support with get/set method prefixes. */
@JsonSerialize(as = FooGetSet.class)
@JsonDeserialize(builder = AutoValue_FooGetSet.Builder.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.NONE)
@JsonPropertyOrder(alphabetic = true)
@AutoValue
public abstract class FooGetSet {
    @JsonProperty("bar")
    abstract String getBar();

    @JsonProperty("baz")
    abstract List<Integer> getBaz();

    public static Builder builder() {
        return new AutoValue_FooGetSet.Builder();
    }

    @AutoValue.Builder
    @JsonPOJOBuilder(withPrefix = "set")
    public abstract static class Builder {
        public abstract Builder setBar(String bar);

        public abstract Builder setBaz(List<Integer> baz);

        public abstract FooGetSet build();
    }
}
