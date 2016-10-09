package org.judal.storage.java.test;

import java.math.BigDecimal;
import java.util.Date;

import org.judal.storage.Record;

public interface TestRecord1 extends Record {

	Integer getId();
	
	void setId(Integer n);

	Date getCreated();

	void setCreated(long timeMilis);

	String getName();

	void setName(String n);

	String getLocation();

	void setLocation(String n);

	byte[] getImage();

	void setImage(byte[] bytes);

	BigDecimal getAmount();

	void setAmount(BigDecimal n);
	
}