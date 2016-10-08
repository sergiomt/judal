package com.sun.java.xml.ns.jdo.jdo.test;

import org.junit.Test;
import static org.junit.Assert.assertEquals;

import java.io.IOException;
import java.io.InputStream;

import org.jibx.runtime.BindingDirectory;
import org.jibx.runtime.IBindingFactory;
import org.jibx.runtime.IUnmarshallingContext;
import org.jibx.runtime.JiBXException;

import com.sun.java.xml.ns.jdo.jdo.Jdo;
import com.sun.java.xml.ns.jdo.jdo.Jdo.Choice;
import com.sun.java.xml.ns.jdo.jdo._Package;

public class TestJDO {

	@Test
	public void testParseXmlMetadata() throws JiBXException, IOException, ClassNotFoundException {		
		IBindingFactory bfact = BindingDirectory.getFactory(Jdo.class);				
	    IUnmarshallingContext uctx = bfact.createUnmarshallingContext();
	    InputStream oInStream = getClass().getResourceAsStream("metadata.xml");
	    Jdo obj = (Jdo) uctx.unmarshalDocument (oInStream, "UTF8");
	    oInStream.close();
	    
	    assertEquals("public",obj.getAttlistJdo().getSchema());
	    
	    assertEquals(1, obj.getChoiceList().size());

	    for (Choice c : obj.getChoiceList()) {
	    	_Package p = c.getPackage();
	    	assertEquals("org.judal.storage.test", p.getAttlistPackage().getName());
	    }
	}
	
}
