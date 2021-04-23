package software.amazon.neptune.gremlin.resultset;

import software.amazon.neptune.common.gremlindatamodel.resultset.ResultSetGetTableTypes;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Gremlin ResultSet class for getTableTypes.
 */
public class GremlinResultSetGetTableTypes extends ResultSetGetTableTypes {
    /**
     * Constructor for GremlinResultSetGetTableTypes.
     *
     * @param statement Statement Object.
     */
    public GremlinResultSetGetTableTypes(final Statement statement) {
        super(statement);
    }

    @Override
    protected ResultSetMetaData getResultMetadata() {
        final List<Class<?>> rowTypes = new ArrayList<>();
        for (int i = 0; i < getColumns().size(); i++) {
            rowTypes.add(String.class);
        }
        return new GremlinResultSetMetadata(getColumns(), rowTypes);
    }
}
