package bio.terra.service.filedata;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.annotation.JsonPropertyOrder;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonPOJOBuilder;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import com.google.auto.value.AutoValue;

import java.util.List;

/** Example AutoValue class with Jackson serialization support. */
@JsonSerialize(as = Foo.class)
@JsonDeserialize(builder = AutoValue_Foo.Builder.class)
@JsonTypeInfo(use = JsonTypeInfo.Id.NONE)
@JsonPropertyOrder(alphabetic = true)
@AutoValue
public abstract class Foo {
    @JsonProperty("bar")
    abstract String bar();

    @JsonProperty("baz")
    abstract List<Integer> baz();

    public static Builder builder() {
        return new AutoValue_Foo.Builder();
    }

    @AutoValue.Builder
    @JsonPOJOBuilder(withPrefix = "")
    public abstract static class Builder {
        public abstract Builder bar(String bar);

        public abstract Builder baz(List<Integer> baz);

        public abstract Foo build();
    }
}
