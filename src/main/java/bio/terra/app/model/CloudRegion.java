package bio.terra.app.model;

import bio.terra.common.EnumUtils;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationContext;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.deser.std.StdDeserializer;
import com.fasterxml.jackson.databind.exc.InvalidFormatException;
import java.io.IOException;

/** Represents regions across different cloud providers */
@JsonDeserialize(using = CloudRegion.CloudRegionDeserializer.class)
public interface CloudRegion {

  /** Returns the implementing enumeration name */
  String name();

  /** Return the region string that the cloud provider can understand */
  String getValue();

  class CloudRegionDeserializer extends StdDeserializer<CloudRegion> {

    public CloudRegionDeserializer() {
      this(CloudRegion.class);
    }

    public CloudRegionDeserializer(Class<?> vc) {
      super(vc);
    }

    @Override
    public CloudRegion deserialize(JsonParser jsonParser, DeserializationContext ctxt)
        throws IOException, JsonProcessingException {
      JsonNode node = jsonParser.getCodec().readTree(jsonParser);
      if (node.isNull()) {
        return null;
      }
      if (!node.isTextual()) {
        throw new InvalidFormatException(
            jsonParser, "Invalid representation of CloudRegion", node, CloudRegion.class);
      }
      String value = node.textValue();
      CloudRegion region = EnumUtils.valueOfLenient(GoogleRegion.class, value);
      if (region != null) {
        return region;
      }
      region = EnumUtils.valueOfLenient(AzureRegion.class, value);
      if (region != null) {
        return region;
      }
      throw new InvalidFormatException(
          jsonParser,
          String.format("Unrecognized CloudRegion: %s", value),
          node,
          CloudRegion.class);
    }
  }
}
