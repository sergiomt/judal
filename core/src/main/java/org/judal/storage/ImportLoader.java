package org.judal.storage;

import javax.jdo.JDOException;

import org.judal.metadata.ColumnList;

/**
 * Interface for performing bulk loads into tables
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public interface ImportLoader extends AutoCloseable {

	int MODE_APPEND = 1;
	int MODE_UPDATE = 2;
	int MODE_APPENDUPDATE = 3;
	int WRITE_LOOKUPS = 4;

	/**
	 * Get columns count
	 * @return int
	 */
	int getNumberOfColumns();

	/**
	 * Get array of column names
	 * @return String[]
	 */
	String[] columnNames() throws IllegalStateException;

	/**
	 * Get current value for a column given its index
	 * @param iColumnIndex int [0..columnCount()-1]
	 * @return Object
	 * @throws ArrayIndexOutOfBoundsException
	 */
	Object get(int iColumnIndex) throws ArrayIndexOutOfBoundsException;

	/**
	 * Get current value for a column given its name
	 * @param sColumnName Case insensitive String
	 * @return Object
	 * @throws ArrayIndexOutOfBoundsException if no column with such name was found
	 */
	Object get(String sColumnName) throws ArrayIndexOutOfBoundsException;

	/**
	 * Get column index from its name
	 * @param sColumnName String
	 * @return int [0..columnCount()-1] or -1 if column was not found
	 */
	int getColumnIndex(String sColumnName);

	/**
	 * Put current value for a column
	 * @param iColumnIndex int [0..columnCount()-1]
	 * @param oValue Object
	 * @throws ArrayIndexOutOfBoundsException
	 */
	void put(int iColumnIndex, Object oValue) throws ArrayIndexOutOfBoundsException;

	/**
	 * Put current value for a column
	 * @param sColumnName String Column name
	 * @param oValue Object
	 * @throws ArrayIndexOutOfBoundsException
	 */
	void put(String sColumnName, Object oValue) throws ArrayIndexOutOfBoundsException;

	/**
	 * Set all current values to null
	 */
	void setAllColumnsToNull();

	/**
	 * Prepare ImportLoader for repeated execution
	 * @param oTbl Table
	 * @param oCols ColumnList List of columns that will be inserted or updated at the database
	 * @throws SQLException
	 */
	void prepare(Table oTbl, ColumnList oCols) throws JDOException;

	/**
	 * <p>Close ImportLoader</p>
	 * Must be always called before ImportLoader is destroyed
	 * @throws SQLException
	 */
	void close() throws JDOException;

	/**
	 * Store a single row or a set of related rows
	 * @param oConn Connection
	 * @param sWorkArea String
	 * @param iFlags int
	 * @throws SQLException
	 * @throws IllegalArgumentException
	 * @throws NullPointerException
	 */
	void store(Table oConn, String sWorkArea, int iFlags) throws JDOException,IllegalArgumentException,NullPointerException;
}
