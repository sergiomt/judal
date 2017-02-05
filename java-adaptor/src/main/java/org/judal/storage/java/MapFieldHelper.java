package org.judal.storage.java;

import java.util.Map;
import java.lang.reflect.InvocationTargetException;

class MapFieldHelper {

	/**
	 * <p>Get value of an HStore field<p>
	 * This method is only supported for PostgreSQL HStore fields
	 * @param sKey JavaRecord Record instance
	 * @param sKey String Field Name
	 * @return Field value or <b>null</b>.
	 * @throws ClassNotFoundException 
	 * @throws SecurityException 
	 * @throws NoSuchMethodException 
	 * @throws InvocationTargetException 
	 * @throws IllegalArgumentException 
	 * @throws IllegalAccessException 
	 * @throws InstantiationException 
	 */
	public static Map<String, String> getMap(JavaRecord oRec, String sKey)
			throws ClassCastException, InstantiationException, IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException {
		if (oRec.isNull(sKey)) {
			return null;
		} else {
			Object oObj = oRec.get(sKey);
			if (oObj instanceof Map)
				return (Map<String, String>) oObj;
			else
				return (Map<String, String>) Class.forName("org.judal.jdbc.HStore").getConstructor(String.class).newInstance(oObj.toString());
		}
	} // getMap

}
