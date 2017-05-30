package org.judal.file;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import javax.jdo.FetchPlan;
import javax.jdo.JDOException;
import javax.jdo.PersistenceManager;

import org.judal.storage.keyvalue.Bucket;
import org.judal.storage.keyvalue.Stored;

import com.knowgate.io.FileUtils;

import static org.judal.storage.DataSource.URI;

public class FileBucket implements Bucket {

	private File dir;
	private String path;
	
	public FileBucket(Map<String, String> properties, String name, boolean create) throws JDOException {
		path = properties.get(URI);
		if (!path.endsWith(File.separator))
			path += File.separator;
		dir = new File(path+name);
		if (!dir.exists())
			if (create)
				dir.mkdirs();
			else
				throw new JDOException("Bucket "+name+" not found");
	}

	@Override
	public String name() {
		return dir.getName();
	}

	@Override
	public boolean exists(Object key) throws JDOException {
		return new File(path+key).exists();
	}

	@Override
	public boolean load(Object key, Stored target) throws JDOException {
		File source = new File(path+key);
		boolean retval = source.exists();
		try {
			if (retval)
				target.setContent(FileUtils.readFileToByteArray(source), null);
			else
				target.setContent(null, null);
		} catch (IOException ioe) {
			throw new JDOException(ioe.getMessage(), ioe);
		}
		return retval;
	}

	@Override
	public void close() throws JDOException {
	}

	@Override
	public void setClass(Class<? extends Stored> candidateClass) {
	}

	@Override
	public void close(Iterator<Stored> arg0) {
	}

	@Override
	public void closeAll() {
	}

	@Override
	public Class<Stored> getCandidateClass() {
		return null;
	}

	@Override
	public FetchPlan getFetchPlan() {
		return null;
	}

	@Override
	public PersistenceManager getPersistenceManager() {
		return null;
	}

	@Override
	public boolean hasSubclasses() {
		return false;
	}

	@Override
	public Iterator<Stored> iterator() {
		String[] files = dir.list();
		Stored[] stores = new FileStore[files.length];
		for (int f=0; f<files.length; f++)
			stores[f] = new FileStore(this, files[f]);
		return Arrays.asList(stores).iterator();
	}

	@Override
	public void store(Stored source) throws JDOException {
		byte[] content = (byte[]) source.getValue();
		File file = new File(path+source.getKey());
		if (file.exists())
			file.delete();
		try {
			if (content!=null)
				FileUtils.writeByteArrayToFile(file, content);
		} catch (IOException ioe) {
			throw new JDOException(ioe.getMessage(), ioe);
		}
	}

	@Override
	public void delete(Object key) throws JDOException {
		File file = new File(path+key);
		if (file.exists())
			file.delete();
	}

	public String getPath() {
		return path;
	}
}
