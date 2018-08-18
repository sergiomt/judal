package org.judal.storage.table.impl;

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
import java.math.BigDecimal;
import java.text.DateFormat;
import java.text.DecimalFormat;
import java.text.Format;
import java.text.NumberFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.AbstractMap.SimpleImmutableEntry;

import org.judal.metadata.ColumnDef;
import org.judal.metadata.ViewDef;
import org.judal.serialization.JSONValue;

import com.knowgate.stringutils.XML;

public class SingleObjectColumnRecord extends AbstractSingleColumnRecord {

	private static final long serialVersionUID = 1L;

	private ColumnDef columnDef;

	public SingleObjectColumnRecord(ViewDef tableDef) {
		super(tableDef.getName());
	}

	public SingleObjectColumnRecord(String tableName) {
		super(tableName);
	}

	public SingleObjectColumnRecord(String tableName, String columnName) {
		super(tableName, columnName);
	}

	public void setColumn(ColumnDef colDef) {
		columnDef = colDef;
	}

	@Override
	public ColumnDef getColumn(String colname) throws ArrayIndexOutOfBoundsException {
		return columnDef;
	}

	@Override
	public boolean isEmpty(String colname) {
		return value == null;
	}

	@Override
	public Object apply(String colname) {
		return value;
	}

	@Override
	public Map<String, Object> asMap() {
		HashMap<String,Object> retval = new HashMap<>(3);
		retval.put(columnDef.getName()!=null ? columnDef.getName() : "value", value);
		return retval;
	}

	@Override
	@SuppressWarnings("unchecked")
	public Entry<String, Object>[] asEntries() {
		return new Entry[] { new SimpleImmutableEntry<String,Object>(columnDef.getName()!=null ? columnDef.getName() : "value", value) };
	}

	@Override
	public String toJSON() throws IOException {
		StringBuilder out = new StringBuilder ();
		JSONValue.writeJSONObject(asMap(), out);
		return out.toString();
	}

	@Override
	public String toXML() throws IOException {
		return toXML("",null,null,null);
	}

	@Override
	public String toXML(String identSpaces, DateFormat dateFormat, NumberFormat decimalFormat, Format textFormat)
			throws IOException {
		return toXML("",null,null,null,null);
	}

	public String toXML(String identSpaces, Map<String,String> attribs, DateFormat dateFormat, NumberFormat decimalFormat, Format textFormat)
			throws IOException {
		String ident = identSpaces==null ? "" : identSpaces;
		final String LF = identSpaces==null ? "" : "\n";
		StringBuilder oBF = new StringBuilder(4000);
		Object oColValue;
		String sColName;
		String sStartElement = identSpaces + identSpaces + "<";
		String sEndElement = ">" + LF;

		DateFormat oXMLDate = dateFormat==null ? new SimpleDateFormat("yyyy-MM-DD HH:mm:ss") : dateFormat;
		NumberFormat oXMLDecimal = decimalFormat==null ? DecimalFormat.getNumberInstance() : decimalFormat;

		String nodeName = getClass().getName();
		int dot = nodeName.lastIndexOf('.');
		if (dot>0)
			nodeName = nodeName.substring(dot+1);

		if (null==attribs) {
			oBF.append(ident + "<" + nodeName + ">" + LF);
		} else {
			oBF.append(ident + "<" + nodeName);
			Iterator<String> oNames = attribs.keySet().iterator();
			while (oNames.hasNext()) {
				String sName = oNames.next();
				oBF.append(" "+sName+"=\""+attribs.get(sName)+"\"");
			} // wend
			oBF.append(">" + LF);
		} // fi

		sColName = columnDef.getName()!=null ? columnDef.getName() : "value";
		oColValue = apply(sColName);

		oBF.append(sStartElement);
		oBF.append(sColName);
		if (null!=oColValue) {
			oBF.append(" isnull=\"false\">");
			if (oColValue instanceof String) {
				if (textFormat==null)
					oBF.append(XML.toCData((String) oColValue));
				else
					oBF.append(XML.toCData(textFormat.format((String) oColValue)));
			} else if (oColValue instanceof java.util.Date) {
				oBF.append(oXMLDate.format((java.util.Date) oColValue));
			} else if (oColValue instanceof Calendar) {
				oBF.append(oXMLDate.format((java.util.Calendar) oColValue));
			} else if (oColValue instanceof BigDecimal) {
				oBF.append(oXMLDecimal.format((BigDecimal) oColValue));
			} else {
				oBF.append(oColValue);
			}
		} else {
			oBF.append(" isnull=\"true\">");
		}
		oBF.append("</").append(sColName).append(sEndElement);

		oBF.append(ident).append("</").append(nodeName).append(">");

		return oBF.toString();
	}

}
