package software.amazon.neptune.common.gremlindatamodel;

import lombok.AllArgsConstructor;
import lombok.Getter;
import java.util.List;
import java.util.Map;

@AllArgsConstructor
public class GraphSchema {
    @Getter
    private final List<String> labels;
    @Getter
    private final List<Map<String, Object>> properties;

    @Override
    public boolean equals(final Object nodeColumnInfo) {
        if (!(nodeColumnInfo instanceof GraphSchema)) {
            return false;
        }
        final GraphSchema nodeInfo = (GraphSchema) (nodeColumnInfo);
        return nodeInfo.labels.equals(this.labels) && nodeInfo.properties.equals(this.properties);
    }
}
