package bio.terra.service.filedata;

import bio.terra.common.category.Unit;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;

@Category(Unit.class)
public class FooGetSetTest {

    @Test
    public void jsonSerialization() throws Exception {
        FooGetSet foo = FooGetSet.builder().setBar("hello world").setBaz(Arrays.asList(1, 2, 3)).build();
        String serialized = "{\"bar\":\"hello world\",\"baz\":[1,2,3]}";

        assertEquals(serialized, new ObjectMapper().writeValueAsString(foo));
        assertEquals(foo, new ObjectMapper().readValue(serialized, FooGetSet.class));
    }
}
