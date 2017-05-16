package org.judal.examples.java.jdbc;

import org.junit.Test;

import com.knowgate.stringutils.Uid;

public class E09_GenerateUniversalUniqueKeys {

	@Test
	@SuppressWarnings("unused")
	public void demo() throws Exception {

		// Generate a Global Unique ID (GUID) of 32 characters length
		String guid32 = Uid.createUniqueKey();
		
		// Create a GUID of 32 characters length that increases on each call
		String sortableGuid32 = Uid.createTimeDependentKey();
		
		// Create a 64 characters GUID which decreases over time
		String guid64 = Uid.generateReverseTimestampId();
		
		// Generate a random (potentially non-unique) Id. of length specified using the given characters
		final int idLength = 8;
		final String charsToUse = "23456789ABCDEFGHJMNPRTXYZ";
		String customId = Uid.generateRandomId(idLength, charsToUse, Character.UNASSIGNED);
	}
	
}
