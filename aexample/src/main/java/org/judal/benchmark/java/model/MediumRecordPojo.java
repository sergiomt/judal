package org.judal.benchmark.java.model;

import java.util.Date;

import javax.jdo.JDOException;

import org.judal.benchmark.java.MediumRecordData;
import org.judal.storage.EngineFactory;
import org.judal.storage.java.PojoRecord;

public class MediumRecordPojo extends PojoRecord {

	private static final long serialVersionUID = 1L;

	public int pk;
	public int i0;
	public int i1;
	public int i2;
	public int i3;
	public int i4;
	public int i5;
	public int i6;
	public int i7;
	public int i8;
	public int i9;
	public Date d0;
	public Date d1;
	public Date d2;
	public Date d3;
	public Date d4;
	public Date d5;
	public Date d6;
	public Date d7;
	public Date d8;
	public Date d9;
	public String v0;
	public String v1;
	public String v2;
	public String v3;
	public String v4;
	public String v5;
	public String v6;
	public String v7;
	public String v8;
	public String v9;
	public String v10;
	public String v11;
	public String v12;
	public String v13;
	public String v14;
	public String v15;
	public String v16;
	public String v17;
	public String v18;
	public String v19;
	public String v20;
	public String v21;
	public String v22;
	public String v23;
	public String v24;
	public String v25;
	public String v26;
	public String v27;
	public String v28;
	public String v29;
	public String v30;
	public String v31;
	public String v32;
	public String v33;
	public String v34;
	public String v35;
	public String v36;
	public String v37;
	public String v38;
	public String v39;
	public String v40;
	public String v41;
	public String v42;
	public String v43;
	public String v44;
	public String v45;
	public String v46;
	public String v47;
	public String v48;
	public String v49;
	
	public MediumRecordPojo() throws JDOException {
		super(EngineFactory.getDefaultRelationalDataSource(), MediumRecordData.TABLE_NAME);
	}
	
	public MediumRecordPojo(MediumRecordData d) throws JDOException {
		super(EngineFactory.getDefaultRelationalDataSource(), MediumRecordData.TABLE_NAME);
		setPk(d.pk);
		setI0(d.ints[0]);
		setI1(d.ints[1]);
		setI2(d.ints[2]);
		setI3(d.ints[3]);
		setI4(d.ints[4]);
		setI5(d.ints[5]);
		setI6(d.ints[6]);
		setI7(d.ints[7]);
		setI8(d.ints[8]);
		setI9(d.ints[9]);
		setD0(d.dates[0]);
		setD1(d.dates[1]);
		setD2(d.dates[2]);
		setD3(d.dates[3]);
		setD4(d.dates[4]);
		setD5(d.dates[5]);
		setD6(d.dates[6]);
		setD7(d.dates[7]);
		setD8(d.dates[8]);
		setD9(d.dates[9]);
		setV0(d.varchars[0]);
		setV1(d.varchars[1]);
		setV2(d.varchars[2]);
		setV3(d.varchars[3]);
		setV4(d.varchars[4]);
		setV5(d.varchars[5]);
		setV6(d.varchars[6]);
		setV7(d.varchars[7]);
		setV8(d.varchars[8]);
		setV9(d.varchars[9]);
		setV10(d.varchars[10]);
		setV11(d.varchars[11]);
		setV12(d.varchars[12]);
		setV13(d.varchars[13]);
		setV14(d.varchars[14]);
		setV15(d.varchars[15]);
		setV16(d.varchars[16]);
		setV17(d.varchars[17]);
		setV18(d.varchars[18]);
		setV19(d.varchars[19]);
		setV20(d.varchars[20]);
		setV21(d.varchars[21]);
		setV22(d.varchars[22]);
		setV23(d.varchars[23]);
		setV24(d.varchars[24]);
		setV25(d.varchars[25]);
		setV26(d.varchars[26]);
		setV27(d.varchars[27]);
		setV28(d.varchars[28]);
		setV29(d.varchars[29]);
		setV30(d.varchars[30]);
		setV31(d.varchars[31]);
		setV32(d.varchars[32]);
		setV33(d.varchars[33]);
		setV34(d.varchars[34]);
		setV35(d.varchars[35]);
		setV36(d.varchars[36]);
		setV37(d.varchars[37]);
		setV38(d.varchars[38]);
		setV39(d.varchars[39]);
		setV40(d.varchars[40]);
		setV41(d.varchars[41]);
		setV42(d.varchars[42]);
		setV43(d.varchars[43]);
		setV44(d.varchars[44]);
		setV45(d.varchars[45]);
		setV46(d.varchars[46]);
		setV47(d.varchars[47]);
		setV48(d.varchars[48]);
		setV49(d.varchars[49]);		
	}

	public int getPk() { return pk; }
	public int getI0() { return i0; }
	public int getI1() { return i1; }
	public int getI2() { return i2; }
	public int getI3() { return i3; }
	public int getI4() { return i4; }
	public int getI5() { return i5; }
	public int getI6() { return i6; }
	public int getI7() { return i7; }
	public int getI8() { return i8; }
	public int getI9() { return i9; }
	public Date getD0() { return d0; }
	public Date getD1() { return d1; }
	public Date getD2() { return d2; }
	public Date getD3() { return d3; }
	public Date getD4() { return d4; }
	public Date getD5() { return d5; }
	public Date getD6() { return d6; }
	public Date getD7() { return d7; }
	public Date getD8() { return d8; }
	public Date getD9() { return d9; }
	public String getV0() { return v0; }
	public String getV1() { return v1; }
	public String getV2() { return v2; }
	public String getV3() { return v3; }
	public String getV4() { return v4; }
	public String getV5() { return v5; }
	public String getV6() { return v6; }
	public String getV7() { return v7; }
	public String getV8() { return v8; }
	public String getV9() { return v9; }
	public String getV10() { return v10; }
	public String getV11() { return v11; }
	public String getV12() { return v12; }
	public String getV13() { return v13; }
	public String getV14() { return v14; }
	public String getV15() { return v15; }
	public String getV16() { return v16; }
	public String getV17() { return v17; }
	public String getV18() { return v18; }
	public String getV19() { return v19; }
	public String getV20() { return v20; }
	public String getV21() { return v21; }
	public String getV22() { return v22; }
	public String getV23() { return v23; }
	public String getV24() { return v24; }
	public String getV25() { return v25; }
	public String getV26() { return v26; }
	public String getV27() { return v27; }
	public String getV28() { return v28; }
	public String getV29() { return v29; }
	public String getV30() { return v30; }
	public String getV31() { return v31; }
	public String getV32() { return v32; }
	public String getV33() { return v33; }
	public String getV34() { return v34; }
	public String getV35() { return v35; }
	public String getV36() { return v36; }
	public String getV37() { return v37; }
	public String getV38() { return v38; }
	public String getV39() { return v39; }
	public String getV40() { return v40; }
	public String getV41() { return v41; }
	public String getV42() { return v42; }
	public String getV43() { return v43; }
	public String getV44() { return v44; }
	public String getV45() { return v45; }
	public String getV46() { return v46; }
	public String getV47() { return v47; }
	public String getV48() { return v48; }
	public String getV49() { return v49; }
	public void setPk(int k) { this.pk = k; }
	public void setI0(int i) { this.i0 = i; }
	public void setI1(int i) { this.i1 = i; }
	public void setI2(int i) { this.i2 = i; }
	public void setI3(int i) { this.i3 = i; }
	public void setI4(int i) { this.i4 = i; }
	public void setI5(int i) { this.i5 = i; }
	public void setI6(int i) { this.i6 = i; }
	public void setI7(int i) { this.i7 = i; }
	public void setI8(int i) { this.i8 = i; }
	public void setI9(int i) { this.i9 = i; }
	public void setD0(Date d) { this.d0 = d; }
	public void setD1(Date d) { this.d1 = d; }
	public void setD2(Date d) { this.d2 = d; }
	public void setD3(Date d) { this.d3 = d; }
	public void setD4(Date d) { this.d4 = d; }
	public void setD5(Date d) { this.d5 = d; }
	public void setD6(Date d) { this.d6 = d; }
	public void setD7(Date d) { this.d7 = d; }
	public void setD8(Date d) { this.d8 = d; }
	public void setD9(Date d) { this.d9 = d; }
	public void setV0(String v) { this.v0 = v; }
	public void setV1(String v) { this.v1 = v; }
	public void setV2(String v) { this.v2 = v; }
	public void setV3(String v) { this.v3 = v; }
	public void setV4(String v) { this.v4 = v; }
	public void setV5(String v) { this.v5 = v; }
	public void setV6(String v) { this.v6 = v; }
	public void setV7(String v) { this.v7 = v; }
	public void setV8(String v) { this.v8 = v; }
	public void setV9(String v) { this.v9 = v; }
	public void setV10(String v) { this.v10 = v; }
	public void setV11(String v) { this.v11 = v; }
	public void setV12(String v) { this.v12 = v; }
	public void setV13(String v) { this.v13 = v; }
	public void setV14(String v) { this.v14 = v; }
	public void setV15(String v) { this.v15 = v; }
	public void setV16(String v) { this.v16 = v; }
	public void setV17(String v) { this.v17 = v; }
	public void setV18(String v) { this.v18 = v; }
	public void setV19(String v) { this.v19 = v; }
	public void setV20(String v) { this.v20 = v; }
	public void setV21(String v) { this.v21 = v; }
	public void setV22(String v) { this.v22 = v; }
	public void setV23(String v) { this.v23 = v; }
	public void setV24(String v) { this.v24 = v; }
	public void setV25(String v) { this.v25 = v; }
	public void setV26(String v) { this.v26 = v; }
	public void setV27(String v) { this.v27 = v; }
	public void setV28(String v) { this.v28 = v; }
	public void setV29(String v) { this.v29 = v; }
	public void setV30(String v) { this.v30 = v; }
	public void setV31(String v) { this.v31 = v; }
	public void setV32(String v) { this.v32 = v; }
	public void setV33(String v) { this.v33 = v; }
	public void setV34(String v) { this.v34 = v; }
	public void setV35(String v) { this.v35 = v; }
	public void setV36(String v) { this.v36 = v; }
	public void setV37(String v) { this.v37 = v; }
	public void setV38(String v) { this.v38 = v; }
	public void setV39(String v) { this.v39 = v; }
	public void setV40(String v) { this.v40 = v; }
	public void setV41(String v) { this.v41 = v; }
	public void setV42(String v) { this.v42 = v; }
	public void setV43(String v) { this.v43 = v; }
	public void setV44(String v) { this.v44 = v; }
	public void setV45(String v) { this.v45 = v; }
	public void setV46(String v) { this.v46 = v; }
	public void setV47(String v) { this.v47 = v; }
	public void setV48(String v) { this.v48 = v; }
	public void setV49(String v) { this.v49 = v; }

}