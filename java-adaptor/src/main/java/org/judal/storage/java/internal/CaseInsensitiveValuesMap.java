package org.judal.storage.java.internal;

import java.util.HashMap;

/**
 * Column names are case insensitive, so specialise a HashMap to implement case insensitive lookups
*/	
public class CaseInsensitiveValuesMap extends HashMap<String,Object> {

	private static final long serialVersionUID = 1L;

	@Override
	public Object put (String key, Object value) {
		return super.put(key.toLowerCase(), value);
	}
	@Override
	public Object remove (Object key) {
		return super.remove(((String) key).toLowerCase());
	}
	@Override
	public Object get (Object key) {
		return super.get(((String) key).toLowerCase());
	}
	@Override
	public boolean containsKey (Object key) {
		return super.containsKey(((String) key).toLowerCase());
	}
}
