package org.judal.storage.java;

import org.judal.storage.keyvalue.AbstractKeyValueOperation;

import static org.judal.storage.java.StoredPojoWrapper.wrap;
import static org.judal.storage.java.StoredPojoWrapper.unwrap;

import javax.jdo.JDOException;

public class PojoKeyValueOperation<R extends PojoRecord> extends AbstractKeyValueOperation<R> {

    @Override
    public PojoRecord load(Object key) throws JDOException {
        StoredPojoWrapper wrapper = new StoredPojoWrapper(key);
        boolean found = getBucket().load(key, wrapper);
        return found ? (PojoRecord) unwrap(wrapper) : null;
    }

    @Override
    public void store() throws JDOException {
        getBucket().store(wrap(getStored()));
    }

}
