package org.judal.storage.test;

import static org.junit.Assert.assertEquals;

import java.rmi.RemoteException;

import javax.jdo.JDOException;
import javax.jdo.JDOUserException;

import org.judal.storage.RecordManager;
import org.judal.storage.TableDataSource;
import org.junit.Test;

import static org.junit.Assert.assertTrue;

public class AbstractRecordManagerTest {

	public static Class<? extends TestRecord2> recordClass2;
	
	protected RecordManager man;

	public static void createTable2(TableDataSource ds) throws JDOException {
		System.out.println("Creating table "+ArrayRecord2.getTableDef(ds).getName());
		ds.createTable(ArrayRecord2.getTableDef(ds), null);
		assertTrue(ds.exists(ArrayRecord2.getTableDef(ds).getName(), "U"));
	}
	
	public void test01Manager() throws InstantiationException, IllegalAccessException, InterruptedException, RemoteException {

		TestRecord2 rec1, rec2, rec3, recr;

		rec1 = recordClass2.newInstance();
		rec1.setCreated(System.currentTimeMillis());
		rec1.setCode("johns");
		rec1.setName("John Smith");
		rec1.setDescription("... ... ...");

		assertEquals("johns", rec1.getKey());
		
		rec2 = recordClass2.newInstance();
		rec2.setCreated(System.currentTimeMillis());
		rec2.setCode("paulb");
		rec2.setName("Paul Browm");
		rec2.setDescription("... ... ...");

		assertEquals("paulb", rec2.getKey());
		
		rec3 = recordClass2.newInstance();
		rec3.setCreated(System.currentTimeMillis());
		rec3.setCode("peters");
		rec3.setName("Peter Scott");
		rec3.setDescription("... ... ...");

		assertEquals("peters", rec3.getKey());
		
		man.makePersistent(rec1);
		man.makePersistent(rec2);
		man.makePersistent(rec3);
		
		Thread.sleep(3000l);
		
		recr = recordClass2.newInstance();		
		recr.setCode("johns");
		assertEquals("johns", recr.getKey());
		man.retrieve(recr);
		assertEquals(rec1.getCreated(), recr.getCreated());
		
		recr = recordClass2.newInstance();		
		recr.setCode("paulb");
		man.retrieve(recr);
		assertEquals(rec2.getCreated(), recr.getCreated());

		recr = recordClass2.newInstance();		
		recr.setCode("peters");
		man.retrieve(recr);
		assertEquals(rec3.getCreated(), recr.getCreated());
		
		recr = (TestRecord2) man.getObjectById("johns");
		assertEquals(rec1.getCreated(), recr.getCreated());

		recr = (TestRecord2) man.getObjectById("paulb");
		assertEquals(rec2.getCreated(), recr.getCreated());

		recr = (TestRecord2) man.getObjectById("peters");
		assertEquals(rec3.getCreated(), recr.getCreated());

	}
	
	public void test02Cache() throws JDOUserException {
		man.getObjectById("Not found in cache");
	}	
}