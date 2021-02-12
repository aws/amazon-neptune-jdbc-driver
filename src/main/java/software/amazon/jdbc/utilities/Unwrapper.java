package software.amazon.jdbc.utilities;

import org.slf4j.Logger;
import java.sql.SQLException;

public class Unwrapper {
    /**
     * Generic unwrap function implementation.
     *
     * @param iface Class Object passed in.
     * @param logger Logger for errors.
     * @param callingClass Calling class of function (should be this).
     * @param <T> Template type of iface.
     * @return Casted Object.
     * @throws SQLException Thrown if it cannot be casted.
     */
    public static <T> T unwrap(final Class<T> iface, final Logger logger, final Object callingClass) throws SQLException {
        if (iface.isAssignableFrom(callingClass.getClass())) {
            return iface.cast(callingClass);
        }

        throw SqlError.createSQLException(
                logger,
                SqlState.DATA_EXCEPTION,
                SqlError.CANNOT_UNWRAP,
                iface.toString());
    }

    /**
     * Generic isWrapperFor implementation.
     *
     * @param iface Class Object passed in.
     * @param callingClass Calling class of function (should be this).
     * @return Whether or not it is assignable.
     */
    public static boolean isWrapperFor(final Class<?> iface, final Object callingClass) {
        return (null != iface) && iface.isAssignableFrom(callingClass.getClass());
    }
}
