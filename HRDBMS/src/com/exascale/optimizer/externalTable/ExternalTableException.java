package com.exascale.optimizer.externalTable;

/**
 * Thrown to indicate error during external source processing
 */
public class ExternalTableException extends RuntimeException {
    /**
     * Constructs an ExternalTableException with no detail message.
     */
    public ExternalTableException(Exception e) {
        super(e);
    }

    /**
     * Constructs an ExternalTableException with the specified
     * detail message.
     *
     * @param message the detail message
     */
    public ExternalTableException(String message) {
        super(message);
    }
}

