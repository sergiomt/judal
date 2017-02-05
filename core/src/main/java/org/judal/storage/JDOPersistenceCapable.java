package org.judal.storage;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;
import java.math.BigDecimal;
import java.text.DecimalFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Locale;

import javax.jdo.FetchGroup;
import javax.jdo.JDOException;
import javax.jdo.PersistenceManager;
import javax.jdo.spi.PersistenceCapable;
import javax.jdo.spi.StateManager;

import org.judal.metadata.ColumnDef;

import com.knowgate.currency.Money;

public class JDOPersistenceCapable implements JDORecord {

	private static final long serialVersionUID = 1L;

	private StateManager man;

	@Override
	public void jdoCopyFields(Object sourceObject, int[] fields) {
	}

	@Override
	public void jdoCopyKeyFieldsFromObjectId(ObjectIdFieldConsumer arg0, Object arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void jdoCopyKeyFieldsToObjectId(Object arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void jdoCopyKeyFieldsToObjectId(ObjectIdFieldSupplier arg0, Object arg1) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object jdoGetObjectId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PersistenceManager jdoGetPersistenceManager() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object jdoGetTransactionalObjectId() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object jdoGetVersion() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean jdoIsDeleted() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean jdoIsDetached() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean jdoIsDirty() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean jdoIsNew() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean jdoIsPersistent() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean jdoIsTransactional() {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void jdoMakeDirty(String arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public PersistenceCapable jdoNewInstance(StateManager arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public PersistenceCapable jdoNewInstance(StateManager arg0, Object arg1) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object jdoNewObjectIdInstance() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object jdoNewObjectIdInstance(Object arg0) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void jdoProvideField(int arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void jdoProvideFields(int[] arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void jdoReplaceField(int arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void jdoReplaceFields(int[] arg0) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void jdoReplaceFlags() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void jdoReplaceStateManager(StateManager arg0) throws SecurityException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public ColumnDef[] columns() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ColumnDef getColumn(String colname) throws ArrayIndexOutOfBoundsException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean isNull(String colname) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean isEmpty(String colname) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub
		
	}

	@Override
	public String getTableName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public FetchGroup fetchGroup() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public ConstraintsChecker getConstraintsChecker() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object apply(String colname) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigDecimal getDecimal(String colname)
			throws NullPointerException, ClassCastException, NumberFormatException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDecimalFormated(String sKey, String sPattern)
			throws ClassCastException, NumberFormatException, NullPointerException, IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDecimalFormated(String sKey, Locale oLoc, int nFractionDigits)
			throws ClassCastException, NumberFormatException, NullPointerException, IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public byte[] getBytes(String colname) throws ClassCastException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getFloatFormated(String colname, String pattern)
			throws ClassCastException, NumberFormatException, NullPointerException, IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer getInteger(String colname) throws NumberFormatException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int getInt(String colname) throws NullPointerException, ClassCastException, NumberFormatException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public long getLong(String colname) throws NullPointerException, NumberFormatException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public Money getMoney(String colname) throws NumberFormatException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public short getShort(String colname) throws NullPointerException, NumberFormatException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public float getFloat(String colname) throws NullPointerException, ClassCastException, NumberFormatException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public double getDouble(String colname) throws NullPointerException, ClassCastException, NumberFormatException {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public String getDoubleFormated(String colname, String sPattern)
			throws ClassCastException, NumberFormatException, NullPointerException, IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate(String colname) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date getDate(String colname, Date defvalue) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDateFormated(String colname, String format) throws ClassCastException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDateTime(String colname) throws ClassCastException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getDateShort(String colname) throws ClassCastException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getString(String colname) throws ClassCastException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getString(String colname, String defvalue) throws ClassCastException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getStringHtml(String colname, String defvalue) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean getBoolean(String colname, boolean defvalue) throws ClassCastException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public Object put(int colpos, Object obj) throws IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object put(String colname, Object obj) throws IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object replace(String colname, Object obj) throws IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Boolean put(String colname, boolean boolval) throws IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object put(String colname, byte[] bytearray) throws IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Byte put(String colname, byte bytevalue) throws IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Short put(String colname, short shortvalue) throws IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Integer put(String colname, int intvalue) throws IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Long put(String colname, long longvalue) throws IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Float put(String colname, float floatvalue) throws IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Double put(String colname, double doublevalue) throws IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Date put(String colname, String strdate, SimpleDateFormat pattern)
			throws ParseException, IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public BigDecimal put(String colname, String decimalvalue, DecimalFormat pattern)
			throws ParseException, IllegalArgumentException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object remove(String colname) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public int size() {
		// TODO Auto-generated method stub
		return 0;
	}

	@Override
	public void setKey(Object key) throws JDOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object getKey() throws JDOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void setValue(Serializable value) throws JDOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object getValue() throws JDOException {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public String getBucketName() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean load(DataSource dataSource, Object key) throws JDOException {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void store(DataSource dataSource) throws JDOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public void delete(DataSource dataSource) throws JDOException {
		// TODO Auto-generated method stub
		
	}

	@Override
	public Object getMap(String colname)
			throws ClassCastException, InstantiationException, IllegalAccessException, IllegalArgumentException,
			InvocationTargetException, NoSuchMethodException, SecurityException, ClassNotFoundException {
		// TODO Auto-generated method stub
		return null;
	}

}
