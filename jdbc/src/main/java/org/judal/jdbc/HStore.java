package org.judal.jdbc;

/*
 * © Copyright 2016 the original author.
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */

import java.sql.SQLException;
import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.NoSuchElementException;

public class HStore implements Iterable<Map.Entry<String, String>> {

	@SuppressWarnings("unused")
	private static final long serialVersionUID = -2491617655490561600L;
	
	private int length;
	private String type;
	private String value;

	public HStore() {
		this.type = "hstore";
		this.length = 0;
	}
	
	public HStore(String rawValue) {
		this.type = "hstore";
		this.value = rawValue;
		this.length = rawValue == null ? 0 : rawValue.length();
	}

	public HStore(Map<String,String> mapValue) {
		this.type = "hstore";
		mapToValue(mapValue);
		this.length = this.value == null ? 0 : this.value.length();
	}

	@SuppressWarnings("unchecked")
	public HStore(Object objValue) {
		this.type = "hstore";
		if (null==objValue)
			this.value = null;
		else if (objValue.getClass().getName().equals("org.postgresql.util.PGobject"))
			this.value = objValue.toString();
		else if (objValue instanceof String)
			this.value = (String) objValue;
		else if (objValue instanceof Map)
			mapToValue((Map<String,String>) objValue);
		this.length = this.value == null ? 0 : this.value.length();
	}

	private void mapToValue(Map<String,String> mapValue) {
		StringBuffer rawValue = new StringBuffer(mapValue.size()*100);
		boolean first = true;
		for (Map.Entry<String, String> entry : mapValue.entrySet()) {
			if (first)
				first = false;
			else
				rawValue.append(COMMA);
			rawValue.append(QUOTE);
			rawValue.append(entry.getKey());
			rawValue.append(QUOTE);
			rawValue.append("=>");
			rawValue.append(QUOTE);
			rawValue.append(quoteQuotes(entry.getValue()));
			rawValue.append(QUOTE);
		}
		this.value = rawValue.toString();
	}

	private StringBuffer quoteQuotes(String input) {
		if (null==input) return null;
		StringBuffer output = new StringBuffer(input.length()+16);
		final int len = input.length();
		for (int n=0; n<len; n++) {
			char c = input.charAt(n);
			if (c=='"')
				output.append('"');
			output.append(c);
		}
		return output;
	}

	public String getValue() {
		return this.value;
	}
	
	public void setValue(String rawValue) {
		if ( ! "hstore".equals(this.type) ) throw new IllegalStateException("HStore database type name should be 'hstore'");
		this.value = rawValue;
		this.length = rawValue == null ? 0 : rawValue.length();
	}
	
	public Map<String,String> asMap() {
		HashMap<String, String> r = new HashMap<String, String>();
		try {
			for (final HStoreIterator iterator = new HStoreIterator(); iterator.hasNext();) {
				final HStoreEntry entry = iterator.rawNext();
				r.put(entry.key, entry.value);
			}
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
		return r;
	}

	private static class HStoreEntry implements Entry<String,String> {
		private String key;
		private String value;
		
		HStoreEntry(String key, String value) {
			this.key = key;
			this.value = value;
		}
		
		@Override
		public String getKey() {
			return key;
		}

		@Override
		public String getValue() {
			return value;
		}

		@Override
		public String setValue(String value) {
			final String oldValue = this.value;
			this.value = value;
			return oldValue;
		}
		
	}

	private static enum ParseState {
		WaitingForKey, WaitingForEquals, WaitingForGreater, WaitingForValue, WaitingForComma
	}

	private static final char QUOTE = '"';
	private static final char EQUALS = '=';
	private static final char GREATER = '>';
	private static final char COMMA = ',';
	private static final String NULL = "NULL";	
	
	private class HStoreIterator implements Iterator<Map.Entry<String, String>> {
		
		private int position;
		private HStoreEntry lastReturned;
		private HStoreEntry nextEntry;
	
	
		public HStoreIterator() throws SQLException {
			this.position = -1;
			advance();
		}
	
		@Override
		public boolean hasNext() {
			return nextEntry != null;
		}

		private HStoreEntry rawNext() throws NoSuchElementException, SQLException {
			if (nextEntry == null)
				throw new NoSuchElementException();
			lastReturned = nextEntry;
			advance();
			return lastReturned;
		}
		
		@Override
		public Entry<String, String> next() throws NoSuchElementException, IllegalStateException {
			try {
				return rawNext();
			} catch (SQLException e) {
				throw new IllegalStateException(e);
			}
		}
	
		/**
		 * Advance in parsing the rawValue string and assign the nextValue
		 * It creates a new nextElement or assigns null to it, if there are no more elements
		 * @throws SQLException 
		 */
		private void advance() throws SQLException {
			String elementKey = null;
			String elementValue = null;
			ParseState state = ParseState.WaitingForKey;
			loop:
			while( position < length - 1 ) {
				final char ch = value.charAt(++position);
				switch (state) {
				case WaitingForKey:
					if ( Character.isWhitespace(ch) ) continue;
					if ( ch == QUOTE ) {
						elementKey = advanceQuoted();
					} else {
						// we have non-quote char, so start loading the key
						elementKey = advanceWord(EQUALS);
						// hstore does not support NULL keys, so NULLs are loaded as usual strings
					}
					state = ParseState.WaitingForEquals;
					continue;
				case WaitingForEquals:
					if ( Character.isWhitespace(ch) ) continue;
					if ( ch == EQUALS ) {
						state = ParseState.WaitingForGreater;
						continue;
					} else {
						throw new SQLException("Expected '=>' key-value separator position "+String.valueOf(position));
					}
				case WaitingForGreater:
					if ( ch == GREATER ) {
						state = ParseState.WaitingForValue;
						continue;
					} else {
						throw new SQLException("Expected '=>' key-value separator position "+String.valueOf(position));
					}
				case WaitingForValue:
					if ( Character.isWhitespace(ch) ) continue;
					if ( ch == QUOTE ) {
						elementValue = advanceQuoted();
					} else {
						// we have non-quote char, so start loading the key
						elementValue = advanceWord(COMMA);
						// hstore supports NULL values, so if unquoted NULL is there, it is rewritten to null
						if ( NULL.equalsIgnoreCase(elementValue) ) {
							elementValue = null;
						}
					}
					state = ParseState.WaitingForComma;
					continue;
				case WaitingForComma:
					if ( Character.isWhitespace(ch) ) continue;
					if ( ch == COMMA ) {
						// we are done
						break loop;
					} else {
						throw new SQLException("Cannot find comma as an end of the value at position "+String.valueOf(position));
					}
				default:
					throw new IllegalStateException("Unknown HStoreParser state");
				}
			} // loop
			// here we either consumed whole string or we found a comma
			if ( state == ParseState.WaitingForKey ) {
				// string was consumed when waiting for key, so we are done with processing
				nextEntry = null;
				return;
			}
			if ( state != ParseState.WaitingForComma ) {
				throw new SQLException("Unexpected end of string at position "+String.valueOf(position));
			}
			if ( elementKey == null ) {
				throw new SQLException("Internal parsing error at position "+String.valueOf(position));
			}
			// init nextValue
			nextEntry = new HStoreEntry(elementKey, elementValue);
		}
	
		private String advanceQuoted() throws SQLException {
			final int firstQuotePosition = position;
			StringBuilder sb = null;
			boolean insideQuote = true;
			while( position < length - 1 ) {
				char ch = value.charAt(++position);
				if ( ch == QUOTE ) {
					// we saw a quote, it is either a closing quote, or it is a quoted quote
					final int nextPosition = position + 1;
					if ( nextPosition < length ) {
						final char nextCh = value.charAt(nextPosition);
						if ( nextCh == QUOTE ) {
							// it was a double quote, so we have to push a quote into the result
							if ( sb == null ) { 
								sb = new StringBuilder(value.substring(firstQuotePosition + 1, nextPosition));
							} else {
								sb.append(QUOTE);
							}
							position++;
							continue;
						}
					}
					// it was a closing quote as we either ware are at the end of the rawValue string
					// or we could not find the next quote
					insideQuote = false;
					break;
				} else {
					if ( sb != null ) {
						sb.append(ch);
					}
				}
			}
			if ( insideQuote ) throw new SQLException("Quote at string position " + firstQuotePosition + " is not closed at position "+String.valueOf(position));
			if ( sb == null ) {
				// we consumed the last quote
				String r = value.substring(firstQuotePosition + 1, position );
				return r;
			} else {
				return sb.toString();
			}
		}
		
		private String advanceWord(final char stopAtChar) throws SQLException {
			final int firstWordPosition = position;
			while( position < length ) {
				final char ch = value.charAt(position);
				if ( ch == QUOTE ) {
					throw new SQLException("Unexpected quote in word "+String.valueOf(position));
				} else if ( Character.isWhitespace(ch) || ch == stopAtChar ) {
					break;
				}
				position++;
			}
			// step back as we are already one char away
			position--;
			// substring is using quite a strange way of defining end position
			final String r = value.substring(firstWordPosition, position + 1 );
			return r;
		}
		
		@Override
		public void remove() {
			throw new UnsupportedOperationException();
		}
	}
	
	@Override
	public Iterator<Entry<String, String>> iterator() {
		try {
			return new HStoreIterator();
		} catch (SQLException e) {
			throw new IllegalStateException(e);
		}
	}

}