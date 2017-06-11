package org.judal.serialization;

/*
 * $Id: JSONValue.java,v 1.1 2006/04/15 14:37:04 platform Exp $
 * Created on 2006-4-15
 */

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;


/**
 * @author FangYidong<fangyidong@yahoo.com.cn>
 */
public class JSONValue {
			
    /**
     * Encode an object into JSON text and write it to out.
     * @param value Object
     * @param writer StringBuilder
     */
	public static void writeJSONString(Object value, StringBuilder out) throws IOException {
		if(value == null){
			out.append("null");
			return;
		}
		
		if(value instanceof String){		
            out.append('\"');
			escape((String)value, out);
            out.append('\"');
			return;
		}
		
		if(value instanceof Double){
			if(((Double)value).isInfinite() || ((Double)value).isNaN())
				out.append("null");
			else
				out.append(value.toString());
			return;
		}
		
		if(value instanceof Float){
			if(((Float)value).isInfinite() || ((Float)value).isNaN())
				out.append("null");
			else
				out.append(value.toString());
			return;
		}		
		
		if(value instanceof Number){
			out.append(value.toString());
			return;
		}
		
		if(value instanceof Boolean){
			out.append(value.toString());
			return;
		}
				
		if(value instanceof Map){
			writeJSONObject((Map)value, out);
			return;
		}
		
		if(value instanceof Collection){
			JSONArray.writeJSONString((Collection)value, out);
            return;
		}
		
		if(value instanceof byte[]){
			JSONArray.writeJSONString((byte[])value, out);
			return;
		}
		
		if(value instanceof short[]){
			JSONArray.writeJSONString((short[])value, out);
			return;
		}
		
		if(value instanceof int[]){
			JSONArray.writeJSONString((int[])value, out);
			return;
		}
		
		if(value instanceof long[]){
			JSONArray.writeJSONString((long[])value, out);
			return;
		}
		
		if(value instanceof float[]){
			JSONArray.writeJSONString((float[])value, out);
			return;
		}
		
		if(value instanceof double[]){
			JSONArray.writeJSONString((double[])value, out);
			return;
		}
		
		if(value instanceof boolean[]){
			JSONArray.writeJSONString((boolean[])value, out);
			return;
		}
		
		if(value instanceof char[]){
			JSONArray.writeJSONString((char[])value, out);
			return;
		}
		
		if(value instanceof Object[]){
			JSONArray.writeJSONString((Object[])value, out);
			return;
		}
		
		out.append(value.toString());
	}

	public static void writeJSONObject(Map map, StringBuilder out) throws IOException {
		if(map == null){
			out.append("null");
			return;
		}
		
		boolean first = true;
		Iterator iter=map.entrySet().iterator();
		
        out.append('{');
		while(iter.hasNext()){
            if(first)
                first = false;
            else
                out.append(',');
			Map.Entry entry=(Map.Entry)iter.next();
            out.append('\"');
            escape(String.valueOf(entry.getKey()), out);
            out.append('\"');
            out.append(':');
			writeJSONString(entry.getValue(), out);
		}
		out.append('}');
	}	

    /**
     * @param s - Must not be null.
     * @param sb
     */
    static void escape(String s, StringBuilder sb) {
    	final int len = s.length();
		for(int i=0;i<len;i++){
			char ch=s.charAt(i);
			switch(ch){
			case '"':
				sb.append("\\\"");
				break;
			case '\\':
				sb.append("\\\\");
				break;
			case '\b':
				sb.append("\\b");
				break;
			case '\f':
				sb.append("\\f");
				break;
			case '\n':
				sb.append("\\n");
				break;
			case '\r':
				sb.append("\\r");
				break;
			case '\t':
				sb.append("\\t");
				break;
			case '/':
				sb.append("\\/");
				break;
			default:
                //Reference: http://www.unicode.org/versions/Unicode5.1.0/
				if((ch>='\u0000' && ch<='\u001F') || (ch>='\u007F' && ch<='\u009F') || (ch>='\u2000' && ch<='\u20FF')){
					String ss=Integer.toHexString(ch);
					sb.append("\\u");
					for(int k=0;k<4-ss.length();k++){
						sb.append('0');
					}
					sb.append(ss.toUpperCase());
				}
				else{
					sb.append(ch);
				}
			}
		}//for
	}

}
