package software.aws.neptune.sparql.resultset;

import software.aws.neptune.common.gremlindatamodel.resultset.ResultSetGetTypeInfo;

import java.sql.Statement;
import java.sql.Types;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class SparqlResultSetGetTypeInfo extends ResultSetGetTypeInfo {
    private static final Map<String, Object> XSDDECIMAL_INFO = new HashMap<>();
    private static final Map<String, Object> XSDINTEGER_INFO = new HashMap<>();
    private static final Map<String, Object> XSDNONPOSITIVEINTEGER_INFO = new HashMap<>();;
    private static final Map<String, Object> XSDNONNEGATIVEINTEGER_INFO = new HashMap<>();
    private static final Map<String, Object> XSDPOSITIVEINTEGER_INFO = new HashMap<>();
    private static final Map<String, Object> XSDNEGATIVEINTEGER_INFO = new HashMap<>();
    private static final Map<String, Object> XSDBYTE_INFO = new HashMap<>();
    private static final Map<String, Object> XSDUNSIGNEDBYTE_INFO = new HashMap<>();
    private static final Map<String, Object> XSDDOUBLE_INFO = new HashMap<>();
    private static final Map<String, Object> XSDFLOAT_INFO = new HashMap<>();
    private static final Map<String, Object> XSDLONG_INFO = new HashMap<>();
    private static final Map<String, Object> XSDUNSIGNEDSHORT_INFO = new HashMap<>();
    private static final Map<String, Object> XSDUNSIGNEDINT_INFO = new HashMap<>();
    private static final Map<String, Object> XSDUNSIGNEDLONG_INFO = new HashMap<>();
    private static final Map<String, Object> XSDINT_INFO = new HashMap<>();
    private static final Map<String, Object> XSDSHORT_INFO = new HashMap<>();
    private static final Map<String, Object> XSDBOOLEAN_INFO = new HashMap<>();
    private static final Map<String, Object> XSDDATE_INFO = new HashMap<>();
    private static final Map<String, Object> XSDTIME_INFO = new HashMap<>();
    private static final Map<String, Object> XSDDATETIME_INFO = new HashMap<>();
    private static final Map<String, Object> XSDDATETIMESTAMP_INFO = new HashMap<>();
    private static final Map<String, Object> XSDDURATION_INFO = new HashMap<>();
    private static final Map<String, Object> XSDSTRING_INFO = new HashMap<>();

    private static final List<Map<String, Object>> TYPE_INFO = new ArrayList<>();

    static {
        // The order added to TYPE_INFO matters
        XSDBOOLEAN_INFO.put("TYPE_NAME", "XSDboolean");
        XSDBOOLEAN_INFO.put("DATA_TYPE", Types.BIT);
        XSDBOOLEAN_INFO.put("PRECISION", 1);
        XSDBOOLEAN_INFO.put("LITERAL_PREFIX", null);
        XSDBOOLEAN_INFO.put("LITERAL_SUFFIX", null);
        XSDBOOLEAN_INFO.put("CASE_SENSITIVE", false);
        TYPE_INFO.add(XSDBOOLEAN_INFO);

        XSDBYTE_INFO.put("TYPE_NAME", "XSDbyte");
        XSDBYTE_INFO.put("DATA_TYPE", Types.TINYINT);
        XSDBYTE_INFO.put("PRECISION", 3);
        XSDBYTE_INFO.put("LITERAL_PREFIX", null);
        XSDBYTE_INFO.put("LITERAL_SUFFIX", null);
        XSDBYTE_INFO.put("CASE_SENSITIVE", false);
        XSDBYTE_INFO.put("MINIMUM_SCALE", 0);
        XSDBYTE_INFO.put("MAXIMUM_SCALE", 0);
        XSDBYTE_INFO.put("NUM_PREC_RADIX", 10);
        TYPE_INFO.add(XSDBYTE_INFO);

        XSDINTEGER_INFO.put("TYPE_NAME", "XSDinteger");
        XSDINTEGER_INFO.put("DATA_TYPE", Types.BIGINT);
        XSDINTEGER_INFO.put("PRECISION", 20);
        XSDINTEGER_INFO.put("LITERAL_PREFIX", null);
        XSDINTEGER_INFO.put("LITERAL_SUFFIX", null);
        XSDINTEGER_INFO.put("CASE_SENSITIVE", false);
        XSDINTEGER_INFO.put("MINIMUM_SCALE", 0);
        XSDINTEGER_INFO.put("MAXIMUM_SCALE", 0);
        XSDINTEGER_INFO.put("NUM_PREC_RADIX", 10);
        TYPE_INFO.add(XSDINTEGER_INFO);

        XSDNONPOSITIVEINTEGER_INFO.put("TYPE_NAME", "XSDnonPositiveInteger");
        XSDNONPOSITIVEINTEGER_INFO.put("DATA_TYPE", Types.BIGINT);
        XSDNONPOSITIVEINTEGER_INFO.put("PRECISION", 20);
        XSDNONPOSITIVEINTEGER_INFO.put("LITERAL_PREFIX", null);
        XSDNONPOSITIVEINTEGER_INFO.put("LITERAL_SUFFIX", null);
        XSDNONPOSITIVEINTEGER_INFO.put("CASE_SENSITIVE", false);
        XSDNONPOSITIVEINTEGER_INFO.put("MINIMUM_SCALE", 0);
        XSDNONPOSITIVEINTEGER_INFO.put("MAXIMUM_SCALE", 0);
        XSDNONPOSITIVEINTEGER_INFO.put("NUM_PREC_RADIX", 10);
        TYPE_INFO.add(XSDNONPOSITIVEINTEGER_INFO);

        XSDNONNEGATIVEINTEGER_INFO.put("TYPE_NAME", "XSDnonNegativeInteger");
        XSDNONNEGATIVEINTEGER_INFO.put("DATA_TYPE", Types.BIGINT);
        XSDNONNEGATIVEINTEGER_INFO.put("PRECISION", 20);
        XSDNONNEGATIVEINTEGER_INFO.put("LITERAL_PREFIX", null);
        XSDNONNEGATIVEINTEGER_INFO.put("LITERAL_SUFFIX", null);
        XSDNONNEGATIVEINTEGER_INFO.put("CASE_SENSITIVE", false);
        XSDNONNEGATIVEINTEGER_INFO.put("MINIMUM_SCALE", 0);
        XSDNONNEGATIVEINTEGER_INFO.put("MAXIMUM_SCALE", 0);
        XSDNONNEGATIVEINTEGER_INFO.put("NUM_PREC_RADIX", 10);
        TYPE_INFO.add(XSDNONNEGATIVEINTEGER_INFO);

        XSDPOSITIVEINTEGER_INFO.put("TYPE_NAME", "XSDpositiveInteger");
        XSDPOSITIVEINTEGER_INFO.put("DATA_TYPE", Types.BIGINT);
        XSDPOSITIVEINTEGER_INFO.put("PRECISION", 20);
        XSDPOSITIVEINTEGER_INFO.put("LITERAL_PREFIX", null);
        XSDPOSITIVEINTEGER_INFO.put("LITERAL_SUFFIX", null);
        XSDPOSITIVEINTEGER_INFO.put("CASE_SENSITIVE", false);
        XSDPOSITIVEINTEGER_INFO.put("MINIMUM_SCALE", 0);
        XSDPOSITIVEINTEGER_INFO.put("MAXIMUM_SCALE", 0);
        XSDPOSITIVEINTEGER_INFO.put("NUM_PREC_RADIX", 10);
        TYPE_INFO.add(XSDPOSITIVEINTEGER_INFO);

        XSDNEGATIVEINTEGER_INFO.put("TYPE_NAME", "XSDnegativeInteger");
        XSDNEGATIVEINTEGER_INFO.put("DATA_TYPE", Types.BIGINT);
        XSDNEGATIVEINTEGER_INFO.put("PRECISION", 20);
        XSDNEGATIVEINTEGER_INFO.put("LITERAL_PREFIX", null);
        XSDNEGATIVEINTEGER_INFO.put("LITERAL_SUFFIX", null);
        XSDNEGATIVEINTEGER_INFO.put("CASE_SENSITIVE", false);
        XSDNEGATIVEINTEGER_INFO.put("MINIMUM_SCALE", 0);
        XSDNEGATIVEINTEGER_INFO.put("MAXIMUM_SCALE", 0);
        XSDNEGATIVEINTEGER_INFO.put("NUM_PREC_RADIX", 10);
        TYPE_INFO.add(XSDNEGATIVEINTEGER_INFO);

        XSDUNSIGNEDINT_INFO.put("TYPE_NAME", "XSDunsignedInt");
        XSDUNSIGNEDINT_INFO.put("DATA_TYPE", Types.BIGINT);
        XSDUNSIGNEDINT_INFO.put("PRECISION", 20);
        XSDUNSIGNEDINT_INFO.put("LITERAL_PREFIX", null);
        XSDUNSIGNEDINT_INFO.put("LITERAL_SUFFIX", null);
        XSDUNSIGNEDINT_INFO.put("CASE_SENSITIVE", false);
        XSDUNSIGNEDINT_INFO.put("UNSIGNED_ATTRIBUTE", true);
        XSDUNSIGNEDINT_INFO.put("MINIMUM_SCALE", 0);
        XSDUNSIGNEDINT_INFO.put("MAXIMUM_SCALE", 0);
        XSDUNSIGNEDINT_INFO.put("NUM_PREC_RADIX", 10);
        TYPE_INFO.add(XSDUNSIGNEDINT_INFO);

        XSDLONG_INFO.put("TYPE_NAME", "XSDlong");
        XSDLONG_INFO.put("DATA_TYPE", Types.BIGINT);
        XSDLONG_INFO.put("PRECISION", 20);
        XSDLONG_INFO.put("LITERAL_PREFIX", null);
        XSDLONG_INFO.put("LITERAL_SUFFIX", null);
        XSDLONG_INFO.put("CASE_SENSITIVE", false);
        XSDLONG_INFO.put("MINIMUM_SCALE", 0);
        XSDLONG_INFO.put("MAXIMUM_SCALE", 0);
        XSDLONG_INFO.put("NUM_PREC_RADIX", 10);
        TYPE_INFO.add(XSDLONG_INFO);

        XSDUNSIGNEDLONG_INFO.put("TYPE_NAME", "XSDunsignedLong");
        XSDUNSIGNEDLONG_INFO.put("DATA_TYPE", Types.BIGINT);
        XSDUNSIGNEDLONG_INFO.put("PRECISION", 20);
        XSDUNSIGNEDLONG_INFO.put("LITERAL_PREFIX", null);
        XSDUNSIGNEDLONG_INFO.put("LITERAL_SUFFIX", null);
        XSDUNSIGNEDLONG_INFO.put("CASE_SENSITIVE", false);
        XSDUNSIGNEDLONG_INFO.put("UNSIGNED_ATTRIBUTE", true);
        XSDUNSIGNEDLONG_INFO.put("MINIMUM_SCALE", 0);
        XSDUNSIGNEDLONG_INFO.put("MAXIMUM_SCALE", 0);
        XSDUNSIGNEDLONG_INFO.put("NUM_PREC_RADIX", 10);
        TYPE_INFO.add(XSDUNSIGNEDLONG_INFO);

        XSDDECIMAL_INFO.put("TYPE_NAME", "XSDdecimal");
        XSDDECIMAL_INFO.put("DATA_TYPE", Types.DECIMAL);
        XSDDECIMAL_INFO.put("PRECISION", 15);
        XSDDECIMAL_INFO.put("LITERAL_PREFIX", null);
        XSDDECIMAL_INFO.put("LITERAL_SUFFIX", null);
        XSDDECIMAL_INFO.put("CASE_SENSITIVE", false);
        XSDDECIMAL_INFO.put("MINIMUM_SCALE", 0);
        XSDDECIMAL_INFO.put("MAXIMUM_SCALE", 0);
        XSDDECIMAL_INFO.put("NUM_PREC_RADIX", 10);
        TYPE_INFO.add(XSDDECIMAL_INFO);

        XSDINT_INFO.put("TYPE_NAME", "XSDint");
        XSDINT_INFO.put("DATA_TYPE", Types.INTEGER);
        XSDINT_INFO.put("PRECISION", 11);
        XSDINT_INFO.put("LITERAL_PREFIX", null);
        XSDINT_INFO.put("LITERAL_SUFFIX", null);
        XSDINT_INFO.put("CASE_SENSITIVE", false);
        XSDINT_INFO.put("MINIMUM_SCALE", 0);
        XSDINT_INFO.put("MAXIMUM_SCALE", 0);
        XSDINT_INFO.put("NUM_PREC_RADIX", 10);
        TYPE_INFO.add(XSDINT_INFO);

        XSDUNSIGNEDBYTE_INFO.put("TYPE_NAME", "XSDunsignedByte");
        XSDUNSIGNEDBYTE_INFO.put("DATA_TYPE", Types.INTEGER);
        XSDUNSIGNEDBYTE_INFO.put("PRECISION", 11);
        XSDUNSIGNEDBYTE_INFO.put("LITERAL_PREFIX", null);
        XSDUNSIGNEDBYTE_INFO.put("LITERAL_SUFFIX", null);
        XSDUNSIGNEDBYTE_INFO.put("CASE_SENSITIVE", false);
        XSDUNSIGNEDINT_INFO.put("UNSIGNED_ATTRIBUTE", true);
        XSDUNSIGNEDBYTE_INFO.put("MINIMUM_SCALE", 0);
        XSDUNSIGNEDBYTE_INFO.put("MAXIMUM_SCALE", 0);
        XSDUNSIGNEDBYTE_INFO.put("NUM_PREC_RADIX", 10);
        TYPE_INFO.add(XSDUNSIGNEDBYTE_INFO);

        XSDUNSIGNEDSHORT_INFO.put("TYPE_NAME", "XSDunsignedShort");
        XSDUNSIGNEDSHORT_INFO.put("DATA_TYPE", Types.INTEGER);
        XSDUNSIGNEDSHORT_INFO.put("PRECISION", 11);
        XSDUNSIGNEDSHORT_INFO.put("LITERAL_PREFIX", null);
        XSDUNSIGNEDSHORT_INFO.put("LITERAL_SUFFIX", null);
        XSDUNSIGNEDSHORT_INFO.put("CASE_SENSITIVE", false);
        XSDUNSIGNEDSHORT_INFO.put("UNSIGNED_ATTRIBUTE", true);
        XSDUNSIGNEDSHORT_INFO.put("MINIMUM_SCALE", 0);
        XSDUNSIGNEDSHORT_INFO.put("MAXIMUM_SCALE", 0);
        XSDUNSIGNEDSHORT_INFO.put("NUM_PREC_RADIX", 10);
        TYPE_INFO.add(XSDUNSIGNEDSHORT_INFO);

        XSDSHORT_INFO.put("TYPE_NAME", "XSDshort");
        XSDSHORT_INFO.put("DATA_TYPE", Types.SMALLINT);
        XSDSHORT_INFO.put("PRECISION", 5);
        XSDSHORT_INFO.put("LITERAL_PREFIX", null);
        XSDSHORT_INFO.put("LITERAL_SUFFIX", null);
        XSDSHORT_INFO.put("CASE_SENSITIVE", false);
        XSDSHORT_INFO.put("MINIMUM_SCALE", 0);
        XSDSHORT_INFO.put("MAXIMUM_SCALE", 0);
        XSDSHORT_INFO.put("NUM_PREC_RADIX", 10);
        TYPE_INFO.add(XSDSHORT_INFO);

        XSDFLOAT_INFO.put("TYPE_NAME", "XSDfloat");
        XSDFLOAT_INFO.put("DATA_TYPE", Types.REAL);
        XSDFLOAT_INFO.put("PRECISION", 15);
        XSDFLOAT_INFO.put("LITERAL_PREFIX", null);
        XSDFLOAT_INFO.put("LITERAL_SUFFIX", null);
        XSDFLOAT_INFO.put("CASE_SENSITIVE", false);
        XSDFLOAT_INFO.put("MINIMUM_SCALE", 0);
        XSDFLOAT_INFO.put("MAXIMUM_SCALE", 0);
        XSDFLOAT_INFO.put("NUM_PREC_RADIX", 10);
        TYPE_INFO.add(XSDFLOAT_INFO);

        XSDDOUBLE_INFO.put("TYPE_NAME", "XSDdouble");
        XSDDOUBLE_INFO.put("DATA_TYPE", Types.DOUBLE);
        XSDDOUBLE_INFO.put("PRECISION", 15);
        XSDDOUBLE_INFO.put("LITERAL_PREFIX", null);
        XSDDOUBLE_INFO.put("LITERAL_SUFFIX", null);
        XSDDOUBLE_INFO.put("CASE_SENSITIVE", false);
        XSDDOUBLE_INFO.put("MINIMUM_SCALE", 0);
        XSDDOUBLE_INFO.put("MAXIMUM_SCALE", 0);
        XSDDOUBLE_INFO.put("NUM_PREC_RADIX", 10);
        TYPE_INFO.add(XSDDOUBLE_INFO);

        XSDSTRING_INFO.put("TYPE_NAME", "XSDstring");
        XSDSTRING_INFO.put("DATA_TYPE", Types.VARCHAR);
        XSDSTRING_INFO.put("PRECISION", Integer.MAX_VALUE);
        XSDSTRING_INFO.put("LITERAL_PREFIX", "'");
        XSDSTRING_INFO.put("LITERAL_SUFFIX", "'");
        XSDSTRING_INFO.put("CASE_SENSITIVE", true);
        TYPE_INFO.add(XSDSTRING_INFO);

        XSDDURATION_INFO.put("TYPE_NAME", "XSDduration");
        XSDDURATION_INFO.put("DATA_TYPE", Types.VARCHAR);
        XSDDURATION_INFO.put("PRECISION", Integer.MAX_VALUE);
        XSDDURATION_INFO.put("LITERAL_PREFIX", "'");
        XSDDURATION_INFO.put("LITERAL_SUFFIX", "'");
        XSDDURATION_INFO.put("CASE_SENSITIVE", true);
        TYPE_INFO.add(XSDDURATION_INFO);

        XSDDATE_INFO.put("TYPE_NAME", "XSDdate");
        XSDDATE_INFO.put("DATA_TYPE", Types.DATE);
        XSDDATE_INFO.put("PRECISION", 24);
        XSDDATE_INFO.put("LITERAL_PREFIX", null);
        XSDDATE_INFO.put("LITERAL_SUFFIX", null);
        XSDDATE_INFO.put("CASE_SENSITIVE", false);
        TYPE_INFO.add(XSDDATE_INFO);

        XSDTIME_INFO.put("TYPE_NAME", "XSDtime");
        XSDTIME_INFO.put("DATA_TYPE", Types.TIME);
        XSDTIME_INFO.put("PRECISION", 24);
        XSDTIME_INFO.put("LITERAL_PREFIX", null);
        XSDTIME_INFO.put("LITERAL_SUFFIX", null);
        XSDTIME_INFO.put("CASE_SENSITIVE", false);
        TYPE_INFO.add(XSDTIME_INFO);

        XSDDATETIMESTAMP_INFO.put("TYPE_NAME", "XSDdateTimeStamp");
        XSDDATETIMESTAMP_INFO.put("DATA_TYPE", Types.TIMESTAMP);
        XSDDATETIMESTAMP_INFO.put("PRECISION", 24);
        XSDDATETIMESTAMP_INFO.put("LITERAL_PREFIX", null);
        XSDDATETIMESTAMP_INFO.put("LITERAL_SUFFIX", null);
        XSDDATETIMESTAMP_INFO.put("CASE_SENSITIVE", false);
        TYPE_INFO.add(XSDDATETIMESTAMP_INFO);

        XSDDATETIME_INFO.put("TYPE_NAME", "XSDdateTime");
        XSDDATETIME_INFO.put("DATA_TYPE", Types.TIMESTAMP);
        XSDDATETIME_INFO.put("PRECISION", 24);
        XSDDATETIME_INFO.put("LITERAL_PREFIX", null);
        XSDDATETIME_INFO.put("LITERAL_SUFFIX", null);
        XSDDATETIME_INFO.put("CASE_SENSITIVE", false);
        TYPE_INFO.add(XSDDATETIME_INFO);

        populateConstants(TYPE_INFO);
    }

    public SparqlResultSetGetTypeInfo(Statement statement) {
        super(statement, new ArrayList<>(TYPE_INFO));
    }
}
