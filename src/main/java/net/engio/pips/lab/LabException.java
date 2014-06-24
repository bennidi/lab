package net.engio.pips.lab;

/**
 * @author bennidi
 *         Date: 3/25/14
 */
public class LabException extends RuntimeException {

    private ErrorCode code;

    public LabException(String message, ErrorCode code) {
        super(message);
        this.code = code;
    }

    public LabException(String message, Throwable cause, ErrorCode code) {
        super(message, cause);
        this.code = code;
    }

    public ErrorCode getCode() {
        return code;
    }

    public static enum ErrorCode{
        WLWithoutFactory,
        WLWithCycleInDuration,
        WLWithCycleInStart,
        WLWithoutStart,
        WLWithoutDuration
    }
}
