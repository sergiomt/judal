package org.judal.storage.keyvalue;

import javax.jdo.JDOException;

public interface KeyValuePair {

    /**
     * <p>Get key.</p>
     * @return Object
     * @throws JDOException
     */
    Object getKey() throws JDOException;

    /**
     * <p>Get value.</p>
     * @return Object
     * @throws JDOException
     */
    Object getValue() throws JDOException;

    /**
     * <p>Get name of the bucket where this key-value is stored.</p>
     * @return String
     */
    String getBucketName();
}
