package org.judal.examples.java;

import java.io.IOException;

import org.junit.runner.JUnitCore;
import org.junit.runner.Result;
import org.junit.runner.notification.Failure;

import com.knowgate.typeutils.ClassSpaceHelper;

public class ExamplesSuite {

	private static final String packageName = "org.judal.examples.java.jdbc";

	public static void main(String[] args) throws ClassNotFoundException, IOException {
		JUnitCore jUnitCore = new JUnitCore();
		Class<?>[] tests = removeClass(ClassSpaceHelper.getClassesAtPackage(packageName), packageName + ".E26_StoredProcedureOutParameters$1");
		Result result = jUnitCore.run(tests);
		for (Failure f : result.getFailures())
			System.err.println(f.getException().getClass().getName() + " " + f.getMessage() + " " + f.getDescription());
	}

	private static Class<?>[] removeClass(Class<?>[] array, String className) {
		Class<?>[] newArray;
		final int len = array.length;
		int index = -1;
	    for (int i = 0; i < len && index == -1; i++)
	    	if (className.equals(array[i].getName()))
	    		index = i;
		if (index == -1) {
			newArray = array;
		} else {
			newArray = new Class<?>[len-1];
		    System.arraycopy(array, 0, newArray, 0, index);
		    if (index < len - 1)
		        System.arraycopy(array, index + 1, newArray, index, len - index - 1);
		}
		return newArray;
	}
	
}
