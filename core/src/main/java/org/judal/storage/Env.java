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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.servlet.ServletConfig;

import com.knowgate.stringutils.Str;
import com.knowgate.debug.DebugFile;

import static org.judal.storage.DataSource.PropertyNames;
import static org.judal.storage.DataSource.DefaultValues;

/**
 * <p>Helper methods to read properties from environment to create data sources and other objects.</p>
 * The list of recognized properties and their default values are kept as static variables at
 * org.judal.storage.DataSource class
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
		for (String[] nameValue : DefaultValues)
			if (nameValue[0].equalsIgnoreCase(propertyName))
				return nameValue[1];
		return null;
	}

	/**
	 * <p>Read DataSource properties from ServletConfig parameters into a Map
	 * DataSource property names names are listed at DataSource.PropertyNames</p>
	 * If namespace is not empty then parameter names will be read as namespace.parameterName
	 * @param obj ServletConfig
	 * @param namespace String
	 * @return Map containing the property name as key and its value at ServletConfig or a default value taken from DataSource interface static variables
	 * @throws ClassCastException If obj is not an instance of class javax.servlet.ServletConfig
	 */
	public static Map<String,String> getDataSourcePropertiesFromServletConfig(Object obj, String namespace) throws ClassCastException {
		ServletConfig cfg = (ServletConfig) obj;
		Hashtable<String,String> props = new Hashtable<String,String>();
		if (namespace==null) namespace = "";
		String prefix = namespace.length()==0 ? "" : namespace + ".";
		for (String propName : PropertyNames) {
			String prop = cfg.getInitParameter(prefix + propName);
			if (DebugFile.trace)
				DebugFile.writeln(prop==null ? "init parameter "+propName+" not found" : "read init parameter "+propName+"="+prop);
			if (prop!=null)
				setProperty(props, propName, prop);
		}
		return props;
	}

	/**
	 * <p>Read DataSource properties from a properties input stream into a Map
	 * If namespace is not empty then property names will be read as namespace.parameterName</p>
	 * <p>If a property name is like ${name} then the property will take the value of the System property
	 * as read with Java System.getProperty() method.</p>
	 * @param inStrm InputStream
	 * @param namespace String
	 * @return Map of String keys to String values
	 * @throws IOException
	 */
	public static Map<String,String> getDataSourceProperties(InputStream inStrm, String namespace) throws IOException {
		Hashtable<String,String> props = new Hashtable<String,String>();
		if (DebugFile.trace) {
			DebugFile.writeln("Begin Env.getDataSourceProperties(InputStream, namespace=\"" + namespace + "\")");
			DebugFile.incIdent();
		}
		if (namespace==null) namespace = "";
		String prefix = namespace.length()==0 ? "" : namespace + ".";
		Properties reader = new Properties();
		reader.load(inStrm);
		for (String propName : PropertyNames) {
			String prop = reader.getProperty(prefix + propName);
			if (DebugFile.trace)
				DebugFile.writeln(prop==null ? "property "+propName+" not found" : "read "+propName+"="+prop);
			if (prop!=null) {
				setProperty(props, propName, prop);
			}
		}
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End Env.getDataSourceProperties()");
		}
		return props;
	}

	/**
	 * <p>Read DataSource properties from a properties input stream into a Map
	 * If namespace is not empty then property names will be read as namespace.parameterName</p>
	 * @param inPath Path
	 * @param namespace String
	 * @return Map of String keys to String values
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
	    sBool = sBool.trim();
	    if (sBool.equalsIgnoreCase("true") || sBool.equalsIgnoreCase("yes") || sBool.equalsIgnoreCase("on") || sBool.equals("1"))
	        bRetVal = true;
	    else if (sBool.equalsIgnoreCase("false") || sBool.equalsIgnoreCase("no") || sBool.equalsIgnoreCase("off") || sBool.equals("0"))
	        bRetVal = false;
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
	   * @param iDefault int
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
	  	if ((null==propValue || propValue.isEmpty()) && getDataSourceDefault(propName)!=null) {
	  		props.put(propName, replaceEnvironmentVariables(getDataSourceDefault(propName)));
	  	} else {
	  		props.put(propName, replaceEnvironmentVariables(propValue));
	  	}
	  }

	  private static String replaceEnvironmentVariables(final String prop) {
	  	final Pattern environmentVariable = Pattern.compile("\\x24\\x7B([^}]+)\\x7D");
	  	String replacedProp = prop;
	  	Matcher matcher = environmentVariable.matcher(replacedProp);
	  	while (matcher.find()) {
			String envVarValue = System.getProperty(matcher.group(1));
			if (null!=envVarValue) {
				envVarValue = envVarValue.replace("\\", "\\\\");
				replacedProp = matcher.replaceFirst(envVarValue);
			} else {
				replacedProp = matcher.replaceFirst("");
			}
			matcher = environmentVariable.matcher(replacedProp);
	  	}
	  	return replacedProp;
	  }
}
