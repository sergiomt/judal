package com.pureperfect.ferret.test;

import org.junit.Test;

import java.io.IOException;
import java.net.URL;
import java.util.Enumeration;
import java.util.Set;

import com.pureperfect.ferret.ScanFilter;
import com.pureperfect.ferret.Scanner;
import com.pureperfect.ferret.vfs.PathElement;

public class TestResourceListing {

	final ScanFilter filter = new ScanFilter() {
		@Override
		public boolean accept(final PathElement resource) {
			return resource.getFullPath().replace('\\', '/').indexOf("com/sun")>=0;
		}
 	};

 	@Test
	public void testListPackageResources() throws IOException {
		Scanner scn = new Scanner();
 		final ClassLoader cl = Thread.currentThread().getContextClassLoader();
		final Enumeration<URL> urls = cl.getResources("com/sun/java/xml/ns/jdo/jdo/test/metadata.xml");

		while (urls.hasMoreElements())
		{
			scn.add(urls.nextElement());
		}
		// scn.add(cl);
		Set<? extends PathElement> resources = scn.scan(filter);
		System.out.println("Scan results");
		for (PathElement pe : resources) {
			System.out.println(pe.getName()+" "+pe.getClass().getName()+" "+pe.getFullPath());
		}
	}
}