package sd.common;

public class ProtocolConstants {
    public static final byte MSG_LOGIN = 1;
    public static final byte MSG_REGISTER = 2;
    public static final byte MSG_ADD_SALE = 3;
    public static final byte MSG_AGGREGATE = 4;
    public static final byte MSG_FILTER_EVENTS = 5;
    public static final byte MSG_WAIT_SIMULTANEOUS = 6;
    public static final byte MSG_WAIT_CONSECUTIVE = 7;
    public static final byte MSG_NEW_DAY = 8;
    public static final byte MSG_LOGOUT = 9;

    public static final byte AGG_QUANTITY = 1;
    public static final byte AGG_VOLUME = 2;
    public static final byte AGG_AVG_PRICE = 3;
    public static final byte AGG_MAX_PRICE = 4;

    public static final byte STATUS_OK = 0;
    public static final byte STATUS_ERROR = 1;
}
