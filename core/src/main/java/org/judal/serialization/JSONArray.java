package org.judal.serialization;

/*
 * $Id: JSONArray.java,v 1.1 2006/04/15 14:10:48 platform Exp $
 * Created on 2006-4-10
 */

import java.io.IOException;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;
import java.util.stream.Collectors;

/**
 * A JSON array. JSONObject supports java.util.List interface.
 * 
 * @author FangYidong<fangyidong@yahoo.com.cn>
 */
public class JSONArray extends ArrayList<Object> {
	private static final long serialVersionUID = 3957988303675231981L;
	
	/**
	 * Constructs an empty JSONArray.
	 */
	public JSONArray(){
		super();
	}
	
	/**
	 * Constructs a JSONArray containing the elements of the specified
	 * collection, in the order they are returned by the collection's iterator.
	 * 
	 * @param c the collection whose elements are to be placed into this JSONArray
	 */
	public JSONArray(Collection<Object> c){
		super(c);
	}
	
    /**
     * Encode a list into JSON text and write it to out. 
     * @param collection
     * @param out
     */
	public static void writeJSONString(Collection<Object> collection, StringBuilder out) throws IOException {
		if(collection == null){
			out.append("null");
			return;
		}
		
		boolean first = true;
		Iterator<Object> iter=collection.iterator();
		
        out.append('[');
		while(iter.hasNext()){
            if(first)
                first = false;
            else
                out.append(',');
            
			Object value=iter.next();
			if(value == null){
				out.append("null");
				continue;
			}
			
			JSONValue.writeJSONString(value, out);
		}
		out.append(']');
	}
	
	public void writeJSONString(StringBuilder out) throws IOException{
		writeJSONString(this, out);
	}
	
	/**
	 * Convert a list to JSON text. The result is a JSON array. 
	 * If this list is also a JSONAware, JSONAware specific behaviours will be omitted at this top level.
	 * 
	 * @see org.json.simple.JSONValue#toJSONString(Object)
	 * 
	 * @param collection
	 * @return JSON text, or "null" if list is null.
	 */
	public static String toJSONString(Collection<Object> collection){
		final StringBuilder writer = new StringBuilder();
		
		try {
			writeJSONString(collection, writer);
			return writer.toString();
		} catch(IOException e){
			// This should never happen for a StringWriter
			throw new RuntimeException(e);
		}
	}

	public static void writeJSONString(byte[] array, StringBuilder out) throws IOException{
		if(array == null){
			out.append("null");
		} else if(array.length == 0) {
			out.append("[]");
		} else {
			out.append("[");
			out.append(String.valueOf(array[0]));
			
			for(int i = 1; i < array.length; i++){
				out.append(",");
				out.append(String.valueOf(array[i]));
			}
			
			out.append("]");
		}
	}
	
	public static void writeJSONString(short[] array, StringBuilder out) throws IOException{
		if(array == null){
			out.append("null");
		} else if(array.length == 0) {
			out.append("[]");
		} else {
			out.append("[");
			out.append(String.valueOf(array[0]));
			
			for(int i = 1; i < array.length; i++){
				out.append(",");
				out.append(String.valueOf(array[i]));
			}
			
			out.append("]");
		}
	}
	
	public static void writeJSONString(int[] array, StringBuilder out) throws IOException{
		if(array == null){
			out.append("null");
		} else if(array.length == 0) {
			out.append("[]");
		} else {
			out.append("[");
			out.append(String.valueOf(array[0]));
			
			for(int i = 1; i < array.length; i++){
				out.append(",");
				out.append(String.valueOf(array[i]));
			}
			
			out.append("]");
		}
	}
	
	public static void writeJSONString(long[] array, StringBuilder out) throws IOException{
		if(array == null){
			out.append("null");
		} else if(array.length == 0) {
			out.append("[]");
		} else {
			out.append("[");
			out.append(String.valueOf(array[0]));
			
			for(int i = 1; i < array.length; i++){
				out.append(",");
				out.append(String.valueOf(array[i]));
			}
			
			out.append("]");
		}
	}
	
	public static void writeJSONString(float[] array, StringBuilder out) throws IOException{
		if(array == null){
			out.append("null");
		} else if(array.length == 0) {
			out.append("[]");
		} else {
			out.append("[");
			out.append(String.valueOf(array[0]));
			
			for(int i = 1; i < array.length; i++){
				out.append(",");
				out.append(String.valueOf(array[i]));
			}
			
			out.append("]");
		}
	}

	public static void writeJSONString(double[] array, StringBuilder out) throws IOException{
		if(array == null){
			out.append("null");
		} else if(array.length == 0) {
			out.append("[]");
		} else {
			out.append("[");
			out.append(String.valueOf(array[0]));
			
			for(int i = 1; i < array.length; i++){
				out.append(",");
				out.append(String.valueOf(array[i]));
			}
			
			out.append("]");
		}
	}
	
	public static void writeJSONString(boolean[] array, StringBuilder out) throws IOException{
		if(array == null){
			out.append("null");
		} else if(array.length == 0) {
			out.append("[]");
		} else {
			out.append("[");
			out.append(String.valueOf(array[0]));
			
			for(int i = 1; i < array.length; i++){
				out.append(",");
				out.append(String.valueOf(array[i]));
			}
			
			out.append("]");
		}
	}
	
	public static void writeJSONString(char[] array, StringBuilder out) throws IOException {
		if(array == null){
			out.append("null");
		} else if(array.length == 0) {
			out.append("[]");
		} else {
			out.append("[\"");
			out.append(String.valueOf(array[0]));
			
			for(int i = 1; i < array.length; i++){
				out.append("\",\"");
				out.append(String.valueOf(array[i]));
			}
			
			out.append("\"]");
		}
	}
	
	public static void writeJSONString(Object[] array, StringBuilder out) throws IOException {
		if(array == null){
			out.append("null");
		} else if(array.length == 0) {
			out.append("[]");
		} else {
			out.append("[");
			JSONValue.writeJSONString(array[0], out);
			
			for(int i = 1; i < array.length; i++){
				out.append(",");
				JSONValue.writeJSONString(array[i], out);
			}
			
			out.append("]");
		}
	}

	@Override
	public String toString() {
		return this.stream().map(Object::toString).collect(Collectors.joining(","));
	}
}
