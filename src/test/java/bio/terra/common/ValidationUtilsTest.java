package bio.terra.common;

import bio.terra.category.Unit;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class ValidationUtilsTest {

    @Test
    public void testEmailFormats() throws Exception {
        assert ValidationUtils.isValidEmail("john@somewhere.com");
        assert ValidationUtils.isValidEmail("john.foo@somewhere.com");
        assert ValidationUtils.isValidEmail("john.foo+label@somewhere.com");
        assert ValidationUtils.isValidEmail("john@192.168.1.10");
        assert ValidationUtils.isValidEmail("john+label@192.168.1.10");
        assert ValidationUtils.isValidEmail("john.foo@someserver");
        assert ValidationUtils.isValidEmail("JOHN.FOO@somewhere.com");
        assert !ValidationUtils.isValidEmail("@someserver");
        assert !ValidationUtils.isValidEmail("@someserver.com");
        assert !ValidationUtils.isValidEmail("john@.");
        assert !ValidationUtils.isValidEmail(".@somewhere.com");
    }
}
