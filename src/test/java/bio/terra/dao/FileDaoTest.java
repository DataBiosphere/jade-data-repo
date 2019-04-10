package bio.terra.dao;

import bio.terra.category.Unit;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.AutoConfigureMockMvc;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import static org.hamcrest.Matchers.equalTo;
import static org.junit.Assert.assertThat;

@RunWith(SpringRunner.class)
@SpringBootTest
@AutoConfigureMockMvc
@Category(Unit.class)
public class FileDaoTest {

    @Autowired
    private FileDao fileDao;

    @Test
    public void pathMiscTest() throws Exception {
        String result = fileDao.getContainingDirectoryPath("/foo/bar/fribble");
        assertThat("Valid path", result, equalTo("/foo/bar"));

        result = fileDao.getContainingDirectoryPath("/foo/bar");
        assertThat("Valid path", result, equalTo("/foo"));

        result = fileDao.getContainingDirectoryPath("/foo");
        assertThat("Should be null", result, equalTo(null));
    }

}
