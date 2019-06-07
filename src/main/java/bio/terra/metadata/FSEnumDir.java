package bio.terra.metadata;

import org.apache.commons.lang3.builder.ToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

import java.util.List;

public class FSEnumDir extends FSObjectBase {
    private List<FSObjectBase> contents;

    public FSEnumDir(FSObjectBase base, List<FSObjectBase> contents) {
        super(base);
        this.contents = contents;
    }

    public List<FSObjectBase> getContents() {
        return contents;
    }

    public FSEnumDir contents(List<FSObjectBase> contents) {
        this.contents = contents;
        return this;
    }

    @Override
    public String toString() {
        return new ToStringBuilder(this, ToStringStyle.JSON_STYLE)
            .append("contents", contents)
            .toString();
    }
}
