package bio.terra.app.utils;

import bio.terra.common.category.Unit;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

@Category(Unit.class)
public class TimUtilsTest {

    private static final String[][] CASES =
        {
            {"TerraCore:Donor.TerraCore:hasAge", "a__terraa__corec__a__donorp__a__terraa__corec__hasa__age"},
            {"TerraCore:Age.TerraCore.hasUpperBound",
                "a__terraa__corec__a__agep__a__terraa__corep__hasa__uppera__bound"},
            {"TerraCore:Age.TerraCore.hasLowerBound",
                "a__terraa__corec__a__agep__a__terraa__corep__hasa__lowera__bound"},
            {"TerraCore:Age.TerraCore.hasAgeUnit", "a__terraa__corec__a__agep__a__terraa__corep__hasa__agea__unit"},
            {"TerraCore:Project.rdfs:label", "a__terraa__corec__a__projectp__rdfsc__label"},
            {"TerraCore:BioSample.dct:identifier", "a__terraa__corec__a__bioa__samplep__dctc__identifier"}
        };

    @Test
    public void encode() {
        for (String[] c : CASES) {
            assertThat(TimUtils.encode(c[0]), is(c[1]));
        }
    }

    @Test
    public void decode() {
        for (String[] c : CASES) {
            assertThat(TimUtils.decode(c[1]), is(c[0]));
        }
    }

    @Test
    public void noDecode() {
        assertThat(TimUtils.decode("a__b"), is("a__b"));
    }
}
