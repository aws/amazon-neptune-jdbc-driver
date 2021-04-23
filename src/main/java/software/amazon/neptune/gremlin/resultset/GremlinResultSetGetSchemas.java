package software.amazon.neptune.gremlin.resultset;

import software.amazon.neptune.common.gremlindatamodel.resultset.ResultSetGetSchemas;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * Gremlin ResultSet class for getSchemas.
 */
public class GremlinResultSetGetSchemas extends ResultSetGetSchemas {
    /**
     * Constructor for GremlinResultSetGetSchemas.
     *
     * @param statement Statement Object.
     */
    public GremlinResultSetGetSchemas(final Statement statement) {
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
