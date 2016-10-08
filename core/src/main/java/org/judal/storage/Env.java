package org.judal.storage;

/**
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import static java.nio.file.StandardOpenOption.READ;
import java.util.Hashtable;
import java.util.Map;
import java.util.Properties;

import javax.servlet.ServletConfig;

import com.knowgate.stringutils.Str;
import com.knowgate.debug.DebugFile;

/**
 * <p>Read properties from environment to create data sources and other objects.</p>
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class Env {
	
	/**
	 * <p>Get default value for a property (if present).</p>
	 * @param propertyName String
	 * @return String Default property value or <b>null</b> if property has no default value.
	 */
	public static String getDataSourceDefault(String propertyName) {
		for (String[] nameValue : DataSource.DefaultValues)
			if (nameValue[0].equalsIgnoreCase(propertyName))
				return nameValue[1];
		return null;
	}

	/**
	 * <p>Read DataSource properties from ServletConfig parameters into a Map
	 * DataSource property names names are listed at DataSource.PropertyNames</p>
	 * If namespace is not empty then parameter names will be read as namespace.parameterName
	 * @param cfg ServletConfig
	 * @param namespace String
	 * @return Map containing the property name as key and its value at ServletConfig or a default value taken from DataSource interface static variables
	 */
	public static Map<String,String> getDataSourceProperties(ServletConfig cfg, String namespace) {
		Hashtable<String,String> props = new Hashtable<String,String>();
		if (namespace==null) namespace = "";
		String prefix = namespace.length()==0 ? "" : namespace + ".";
		for (String propName : DataSource.PropertyNames)
			setProperty(props, propName, cfg.getInitParameter(prefix + propName));
		return props;
	}

	/**
	 * <p>Read DataSource properties from a properties input stream into a Map
	 * If namespace is not empty then property names will be read as namespace.parameterName</p>
	 * @param inStrm InputStream
	 * @param namespace String
	 * @return
	 * @throws IOException
	 */
	public static Map<String,String> getDataSourceProperties(InputStream inStrm, String namespace) throws IOException {
		Hashtable<String,String> props = new Hashtable<String,String>();
		if (namespace==null) namespace = "";
		String prefix = namespace.length()==0 ? "" : namespace + ".";
		Properties reader = new Properties();
		reader.load(inStrm);
		for (String propName : DataSource.PropertyNames)
			setProperty(props, propName, reader.getProperty(prefix + propName));
		return props;
	}

	/**
	 * <p>Read DataSource properties from a properties input stream into a Map
	 * If namespace is not empty then property names will be read as namespace.parameterName</p>
	 * @param inPath Path
	 * @param namespace String
	 * @return
	 * @throws IOException
	 */
	public static Map<String,String> getDataSourceProperties(Path inPath, String namespace) throws IOException {
		InputStream inStrm = Files.newInputStream(inPath, READ);
		Map<String,String> retval = getDataSourceProperties(inStrm, namespace);
		inStrm.close();
		return retval;
	}
	
	  /**
	   * <p>Get the value of a property that represents a boolean type.</p>
	   * @param oProperties Map&lt;String,String&gt;
	   * @param sVarName Property Name
	   * @param bDefault Default Value
	   * @return If no property named sVarName is found at sProfile then bDefault value is returned.
	   * If sVarName is one of {true , yes, on, 1} then return value is <b>true</b>.
	   * If sVarName is one of {false, no, off, 0} then return value is <b>false</b>.
	   * If sVarName is any other value then then return value is bDefault
	   */
	  public static boolean getBoolean(Map<String,String> oProperties, String sVarName, boolean bDefault) {
	    boolean bRetVal = bDefault;
	    String sBool = oProperties.get(sVarName);
	    if (null==sBool)
	      sBool = bDefault ? "true" : "false";
	    if (null!=sBool) {
	      sBool = sBool.trim();
	      if (sBool.equalsIgnoreCase("true") || sBool.equalsIgnoreCase("yes") || sBool.equalsIgnoreCase("on") || sBool.equals("1"))
	        bRetVal = true;
	      else if (sBool.equalsIgnoreCase("false") || sBool.equalsIgnoreCase("no") || sBool.equalsIgnoreCase("off") || sBool.equals("0"))
	        bRetVal = false;
	      else
	        bRetVal = bDefault;      	
	    } // fi
	    return bRetVal;
	  } // getProfileBool

	  /**
	   * <p>Get a property representing a file path.</p>
	   * <p>This method ensures that a file separator is always appended to the end of the readed value.</p>
	   * @param oProperties Map&lt;String,String&gt;
	   * @param sVarName Property Name
	   * @return Value terminated with a file separator or <b>null</b> if no property with such name was found.
	   */
	  public static String getPath(Map<String,String> oProperties, String sVarName) {
	    String sPath = oProperties.get(sVarName);
	    return Str.chomp(sPath, System.getProperty("file.separator"));
	  }

	  public static String getString(Map<String,String> oProperties, String sVarName, String sDefault) {
		String sValue = oProperties.get(sVarName);
		return null==sValue ? sDefault : sValue;
	  }

	  /**
	   * <p>Get a property representing a positive integer value.</p>
	   * <p>This method ensures that a file separator is always appended to the end of the read value.</p>
	   * @param oProperties Map&lt;String,String&gt;
	   * @param sVarName Property Name
	   * @return Value terminated with a file separator or <b>null</b> if no property with such name was found.
	   * @throws NumberFormatException
	   */
	  public static int getPositiveInteger(Map<String,String> oProperties, String sVarName, int iDefault) {
		int iRetVal;
		String sValue = oProperties.get(sVarName);
		if (null==sValue) {
			iRetVal = iDefault;
		} else {
		  try {
			  iRetVal = Integer.parseInt(sValue);
			  if (iRetVal<0) throw new NumberFormatException();
		  }
		  catch (NumberFormatException nfe) {
			if (DebugFile.trace) {
			  DebugFile.writeln(sVarName + " property must be a positive integer value");
			  DebugFile.decIdent();
			}
			throw new NumberFormatException(sVarName + " property must be a positive integer value");
		  }			
		} // fi
		return iRetVal;
	  }

	  private static void setProperty(Hashtable<String,String> props, String propName, String propValue) {
			if (null==propValue) {
				if (getDataSourceDefault(propName)!=null)
					props.put(propName, getDataSourceDefault(propName));
			} else if (propValue.length()==0) {
				if (getDataSourceDefault(propName)!=null)
					props.put(propName, getDataSourceDefault(propName));
			} else {
				props.put(propName, propValue);
			}		
	  }
	  
}
