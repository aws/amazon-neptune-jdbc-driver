package software.amazon.neptune.opencypher.resultset;

import org.neo4j.driver.internal.types.InternalTypeSystem;
import org.neo4j.driver.types.Type;
import software.amazon.neptune.common.gremlindatamodel.resultset.ResultSetGetTableTypes;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

/**
 * OpenCypher ResultSet class for getTableTypes.
 */
public class OpenCypherResultSetGetTableTypes extends ResultSetGetTableTypes {
    /**
     * Constructor for OpenCypherResultSetGetTableTypes.
     *
     * @param statement Statement Object.
     */
    public OpenCypherResultSetGetTableTypes(final Statement statement) {
        super(statement);
    }

    @Override
    protected ResultSetMetaData getResultMetadata() {
        final List<Type> rowTypes = new ArrayList<>();
        for (int i = 0; i < getColumns().size(); i++) {
            rowTypes.add(InternalTypeSystem.TYPE_SYSTEM.STRING());
        }
        return new OpenCypherResultSetMetadata(getColumns(), rowTypes);
    }
}
