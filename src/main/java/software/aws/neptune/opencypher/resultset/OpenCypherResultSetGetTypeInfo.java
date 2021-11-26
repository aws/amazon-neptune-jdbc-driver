package software.aws.neptune.opencypher.resultset;

import software.aws.neptune.common.gremlindatamodel.resultset.ResultSetGetTypeInfo;

import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class OpenCypherResultSetGetTypeInfo extends ResultSetGetTypeInfo {
    private static final Map<String, Object> ANY_INFO = new HashMap<>();
    private static final Map<String, Object> BOOLEAN_INFO = new HashMap<>();
    private static final Map<String, Object> BYTES_INFO = new HashMap<>();;
    private static final Map<String, Object> STRING_INFO = new HashMap<>();
    private static final Map<String, Object> NUMBER_INFO = new HashMap<>();
    private static final Map<String, Object> INTEGER_INFO = new HashMap<>();
    private static final Map<String, Object> FLOAT_INFO = new HashMap<>();
    private static final Map<String, Object> LIST_INFO = new HashMap<>();
    private static final Map<String, Object> MAP_INFO = new HashMap<>();
    private static final Map<String, Object> NODE_INFO = new HashMap<>();
    private static final Map<String, Object> RELATIONSHIP_INFO = new HashMap<>();
    private static final Map<String, Object> PATH_INFO = new HashMap<>();
    private static final Map<String, Object> POINT_INFO = new HashMap<>();
    private static final Map<String, Object> DATE_INFO = new HashMap<>();
    private static final Map<String, Object> TIME_INFO = new HashMap<>();
    private static final Map<String, Object> LOCAL_TIME_INFO = new HashMap<>();
    private static final Map<String, Object> LOCAL_DATE_TIME_INFO = new HashMap<>();
    private static final Map<String, Object> DATE_TIME_INFO = new HashMap<>();
    private static final Map<String, Object> DURATION_INFO = new HashMap<>();
    private static final Map<String, Object> NULL_INFO = new HashMap<>();

    private static final List<Map<String, Object>> TYPE_INFO = new ArrayList<>();

    static {
        // The order added to TYPE_INFO matters
        BOOLEAN_INFO.put("TYPE_NAME", "BOOLEAN");
        BOOLEAN_INFO.put("DATA_TYPE", Types.BIT);
        BOOLEAN_INFO.put("PRECISION", 1);
        BOOLEAN_INFO.put("LITERAL_PREFIX", null);
        BOOLEAN_INFO.put("LITERAL_SUFFIX", null);
        BOOLEAN_INFO.put("CASE_SENSITIVE", false);
        TYPE_INFO.add(BOOLEAN_INFO);

        NULL_INFO.put("TYPE_NAME", "NULL");
        NULL_INFO.put("DATA_TYPE", Types.NULL);
        NULL_INFO.put("PRECISION", 0);
        NULL_INFO.put("LITERAL_PREFIX", null);
        NULL_INFO.put("LITERAL_SUFFIX", null);
        NULL_INFO.put("CASE_SENSITIVE", false);
        TYPE_INFO.add(NULL_INFO);

        INTEGER_INFO.put("TYPE_NAME", "INTEGER");
        INTEGER_INFO.put("DATA_TYPE", Types.INTEGER);
        INTEGER_INFO.put("PRECISION", 11);
        INTEGER_INFO.put("LITERAL_PREFIX", null);
        INTEGER_INFO.put("LITERAL_SUFFIX", null);
        INTEGER_INFO.put("CASE_SENSITIVE", false);
        INTEGER_INFO.put("MINIMUM_SCALE", 0);
        INTEGER_INFO.put("MAXIMUM_SCALE", 0);
        INTEGER_INFO.put("NUM_PREC_RADIX", 10);
        TYPE_INFO.add(INTEGER_INFO);

        NUMBER_INFO.put("TYPE_NAME", "NUMBER");
        NUMBER_INFO.put("DATA_TYPE", Types.DOUBLE);
        NUMBER_INFO.put("PRECISION", 15);
        NUMBER_INFO.put("LITERAL_PREFIX", null);
        NUMBER_INFO.put("LITERAL_SUFFIX", null);
        NUMBER_INFO.put("CASE_SENSITIVE", false);
        NUMBER_INFO.put("MINIMUM_SCALE", 0);
        NUMBER_INFO.put("MAXIMUM_SCALE", 0);
        NUMBER_INFO.put("NUM_PREC_RADIX", 10);
        TYPE_INFO.add(NUMBER_INFO);

        FLOAT_INFO.put("TYPE_NAME", "FLOAT");
        FLOAT_INFO.put("DATA_TYPE", Types.DOUBLE);
        FLOAT_INFO.put("PRECISION", 15);
        FLOAT_INFO.put("LITERAL_PREFIX", null);
        FLOAT_INFO.put("LITERAL_SUFFIX", null);
        FLOAT_INFO.put("CASE_SENSITIVE", false);
        FLOAT_INFO.put("MINIMUM_SCALE", 0);
        FLOAT_INFO.put("MAXIMUM_SCALE", 0);
        FLOAT_INFO.put("NUM_PREC_RADIX", 10);
        TYPE_INFO.add(FLOAT_INFO);

        STRING_INFO.put("TYPE_NAME", "STRING");
        STRING_INFO.put("DATA_TYPE", Types.VARCHAR);
        STRING_INFO.put("PRECISION", Integer.MAX_VALUE);
        STRING_INFO.put("LITERAL_PREFIX", "'");
        STRING_INFO.put("LITERAL_SUFFIX", "'");
        STRING_INFO.put("CASE_SENSITIVE", true);
        TYPE_INFO.add(STRING_INFO);

        ANY_INFO.put("TYPE_NAME", "ANY");
        ANY_INFO.put("DATA_TYPE", Types.VARCHAR);
        ANY_INFO.put("PRECISION", Integer.MAX_VALUE);
        ANY_INFO.put("LITERAL_PREFIX", "'");
        ANY_INFO.put("LITERAL_SUFFIX", "'");
        ANY_INFO.put("CASE_SENSITIVE", true);
        TYPE_INFO.add(ANY_INFO);

        LIST_INFO.put("TYPE_NAME", "LIST");
        LIST_INFO.put("DATA_TYPE", Types.VARCHAR);
        LIST_INFO.put("PRECISION", Integer.MAX_VALUE);
        LIST_INFO.put("LITERAL_PREFIX", "'");
        LIST_INFO.put("LITERAL_SUFFIX", "'");
        LIST_INFO.put("CASE_SENSITIVE", true);
        TYPE_INFO.add(LIST_INFO);

        MAP_INFO.put("TYPE_NAME", "MAP");
        MAP_INFO.put("DATA_TYPE", Types.VARCHAR);
        MAP_INFO.put("PRECISION", Integer.MAX_VALUE);
        MAP_INFO.put("LITERAL_PREFIX", "'");
        MAP_INFO.put("LITERAL_SUFFIX", "'");
        MAP_INFO.put("CASE_SENSITIVE", true);
        TYPE_INFO.add(MAP_INFO);

        NODE_INFO.put("TYPE_NAME", "NODE");
        NODE_INFO.put("DATA_TYPE", Types.VARCHAR);
        NODE_INFO.put("PRECISION", Integer.MAX_VALUE);
        NODE_INFO.put("LITERAL_PREFIX", "'");
        NODE_INFO.put("LITERAL_SUFFIX", "'");
        NODE_INFO.put("CASE_SENSITIVE", true);
        TYPE_INFO.add(NODE_INFO);

        RELATIONSHIP_INFO.put("TYPE_NAME", "RELATIONSHIP");
        RELATIONSHIP_INFO.put("DATA_TYPE", Types.VARCHAR);
        RELATIONSHIP_INFO.put("PRECISION", Integer.MAX_VALUE);
        RELATIONSHIP_INFO.put("LITERAL_PREFIX", "'");
        RELATIONSHIP_INFO.put("LITERAL_SUFFIX", "'");
        RELATIONSHIP_INFO.put("CASE_SENSITIVE", true);
        TYPE_INFO.add(RELATIONSHIP_INFO);

        POINT_INFO.put("TYPE_NAME", "PATH");
        PATH_INFO.put("DATA_TYPE", Types.VARCHAR);
        PATH_INFO.put("PRECISION", Integer.MAX_VALUE);
        PATH_INFO.put("LITERAL_PREFIX", "'");
        PATH_INFO.put("LITERAL_SUFFIX", "'");
        PATH_INFO.put("CASE_SENSITIVE", true);
        TYPE_INFO.add(PATH_INFO);

        POINT_INFO.put("TYPE_NAME", "POINT");
        POINT_INFO.put("DATA_TYPE", Types.VARCHAR);
        POINT_INFO.put("PRECISION", Integer.MAX_VALUE);
        POINT_INFO.put("LITERAL_PREFIX", "'");
        POINT_INFO.put("LITERAL_SUFFIX", "'");
        POINT_INFO.put("CASE_SENSITIVE", true);
        TYPE_INFO.add(POINT_INFO);

        DURATION_INFO.put("TYPE_NAME", "DURATION");
        DURATION_INFO.put("DATA_TYPE", Types.VARCHAR);
        DURATION_INFO.put("PRECISION", Integer.MAX_VALUE);
        DURATION_INFO.put("LITERAL_PREFIX", "'");
        DURATION_INFO.put("LITERAL_SUFFIX", "'");
        DURATION_INFO.put("CASE_SENSITIVE", "'");
        TYPE_INFO.add(DURATION_INFO);

        BYTES_INFO.put("TYPE_NAME", "BYTES");
        BYTES_INFO.put("DATA_TYPE", Types.VARCHAR);
        BYTES_INFO.put("PRECISION", Integer.MAX_VALUE);
        BYTES_INFO.put("LITERAL_PREFIX", null);
        BYTES_INFO.put("LITERAL_SUFFIX", null);
        BYTES_INFO.put("CASE_SENSITIVE", false);
        TYPE_INFO.add(BYTES_INFO);

        DATE_INFO.put("TYPE_NAME", "DATE");
        DATE_INFO.put("DATA_TYPE", Types.DATE);
        DATE_INFO.put("PRECISION", 24);
        DATE_INFO.put("LITERAL_PREFIX", null);
        DATE_INFO.put("LITERAL_SUFFIX", null);
        DATE_INFO.put("CASE_SENSITIVE", false);
        TYPE_INFO.add(DATE_INFO);

        TIME_INFO.put("TYPE_NAME", "TIME");
        TIME_INFO.put("DATA_TYPE", Types.TIME);
        TIME_INFO.put("PRECISION", 24);
        TIME_INFO.put("LITERAL_PREFIX", null);
        TIME_INFO.put("LITERAL_SUFFIX", null);
        TIME_INFO.put("CASE_SENSITIVE", false);
        TYPE_INFO.add(TIME_INFO);

        LOCAL_TIME_INFO.put("TYPE_NAME", "LOCAL_TIME");
        LOCAL_TIME_INFO.put("DATA_TYPE", Types.TIME);
        LOCAL_TIME_INFO.put("PRECISION", 24);
        LOCAL_TIME_INFO.put("LITERAL_PREFIX", null);
        LOCAL_TIME_INFO.put("LITERAL_SUFFIX", null);
        LOCAL_TIME_INFO.put("CASE_SENSITIVE", false);
        TYPE_INFO.add(LOCAL_TIME_INFO);

        DATE_TIME_INFO.put("TYPE_NAME", "DATE_TIME");
        DATE_TIME_INFO.put("DATA_TYPE", Types.TIMESTAMP);
        DATE_TIME_INFO.put("PRECISION", 24);
        DATE_TIME_INFO.put("LITERAL_PREFIX", null);
        DATE_TIME_INFO.put("LITERAL_SUFFIX", null);
        DATE_TIME_INFO.put("CASE_SENSITIVE", false);
        TYPE_INFO.add(DATE_TIME_INFO);

        LOCAL_DATE_TIME_INFO.put("TYPE_NAME", "LOCAL_DATE_TIME");
        LOCAL_DATE_TIME_INFO.put("DATA_TYPE", Types.TIMESTAMP);
        LOCAL_DATE_TIME_INFO.put("PRECISION", 24);
        LOCAL_DATE_TIME_INFO.put("LITERAL_PREFIX", null);
        LOCAL_DATE_TIME_INFO.put("LITERAL_SUFFIX", null);
        LOCAL_DATE_TIME_INFO.put("CASE_SENSITIVE", false);
        TYPE_INFO.add(LOCAL_DATE_TIME_INFO);

        populateConstants(TYPE_INFO);
    }

    public OpenCypherResultSetGetTypeInfo(Statement statement) {
        super(statement, new ArrayList<>(TYPE_INFO));
    }
}
