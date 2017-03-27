package org.judal.metadata.bind;

import java.io.IOException;
import java.io.InputStream;

import java.net.URL;

import java.util.Enumeration;

import javax.jdo.JDOException;

import org.judal.metadata.MetadataScanner;
import org.judal.metadata.SchemaMetaData;
import org.judal.storage.DataSource;

import com.knowgate.debug.DebugFile;
import com.pureperfect.ferret.ScanFilter;
import com.pureperfect.ferret.Scanner;
import com.pureperfect.ferret.vfs.PathElement;

public class JdoPackageMetadata implements MetadataScanner {

	private DataSource dataSource;
	private String packagePath;
	private String xmlFileName;
	
	protected final ScanFilter any = new ScanFilter() {
		@Override
		public boolean accept(final PathElement resource) {
			return true;
		}
 	};
 	
 	public JdoPackageMetadata(DataSource dataSource, String packagePath, String xmlFileName) {
 		this.dataSource = dataSource;
 		this.packagePath = packagePath;
 		this.xmlFileName = xmlFileName;
 	}

 	public String getPackagePath() {
 		return packagePath;
 	}

 	public String getFileName() {
 		return xmlFileName;
 	}

 	public InputStream openStream() throws IOException {
		if (DebugFile.trace) {
			DebugFile.writeln("Begin JdoPackageMetadata.openStream()");
			DebugFile.incIdent();
		}
		InputStream retval;
 		Scanner scn = new Scanner();
 		ClassLoader cl = Thread.currentThread().getContextClassLoader();
		final String resourcePath = getPackagePath()+"/"+(getFileName()==null ? "metadata.xml" : getFileName());
		if (DebugFile.trace)
			DebugFile.writeln("ClassLoader.getResources("+resourcePath+")");
 		Enumeration<URL> urls = cl.getResources(resourcePath);
		if (urls.hasMoreElements()) {
			URL resUrl = urls.nextElement();
			if (DebugFile.trace)
				DebugFile.writeln("Scanner.add("+resUrl.toString()+")");
			scn.add(resUrl);
			PathElement xml = scn.scan(any).iterator().next();
			retval = xml.openStream();
		}  else {
			retval = null;
		}
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End JdoPackageMetadata.openStream() : "+retval);
		}
		return retval;
 	}

	@Override
	public SchemaMetaData readMetadata(InputStream in) throws JDOException, IOException {
		JdoXmlMetadata xmlparser = new JdoXmlMetadata(dataSource);
		SchemaMetaData metadata = xmlparser.readMetadata(in);
		return metadata;
	}
	
}
