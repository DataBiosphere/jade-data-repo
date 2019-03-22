package bio.terra.metadata;

import org.apache.commons.lang3.StringUtils;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;

public class AssetTable {
    private Table studyTable;
    private List<AssetColumn> columns = new ArrayList<>();

    public Table getTable() {
        return studyTable;
    }

    public AssetTable studyTable(Table studyTable) {
        this.studyTable = studyTable;
        return this;
    }

    public Collection<AssetColumn> getColumns() {
        return columns;
    }

    public AssetTable columns(List<AssetColumn> columns) {
        this.columns = columns;
        return this;
    }

    public Optional<Column> getStudyColumnByName(String name) {
        for (AssetColumn assetColumn : getColumns()) {
            Column studyColumn = assetColumn.getStudyColumn();
            if (StringUtils.equals(name, studyColumn.getName())) {
                return Optional.of(studyColumn);
            }
        }
        return Optional.empty();
    }

}
