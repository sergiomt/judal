package org.judal.benchmark.java;

import java.util.Date;
import java.util.Random;

import com.knowgate.stringutils.Uid;

public class MediumRecordData {

	public static final String TABLE_NAME = "mediumrecord1";

	public int pk;
	public int[] ints;
	public Date[] dates;
	public String[] varchars;

	public MediumRecordData() {
		this(true);
	}

	public MediumRecordData(boolean fillData) {
		ints = new int[10];
		dates = new Date[10];
		varchars = new String[50];
		if (fillData) {
			Random r = new Random();
			for (int i=0; i<ints.length; i++)
				ints[i] = r.nextInt();
			for (int d=0; d<dates.length; d++)
				dates[d] = new Date();
			for (int v=0; v<varchars.length; v++)
				varchars[v] = Uid.generateRandomId(r.nextInt(80)+10, null, Character.LOWERCASE_LETTER);
		}
	}

}