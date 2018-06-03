package org.judal.file;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import javax.jdo.FetchPlan;
import javax.jdo.JDOException;
import javax.jdo.PersistenceManager;

import org.judal.storage.Param;
import org.judal.storage.keyvalue.Bucket;
import org.judal.storage.keyvalue.Stored;

import com.knowgate.io.FileUtils;
import com.knowgate.io.MimeUtils;

import static org.judal.storage.DataSource.URI;

public class FileBucket implements Bucket {

	final private File dir;
	final private String path;
	
	public FileBucket(Map<String, String> properties, String name, boolean create) throws JDOException {
		String uri = properties.get(URI);
		if (!uri.endsWith(File.separator))
			uri += File.separator;
		path = uri + name + File.separator;
		dir = new File(path);
		if (!dir.exists())
			if (create)
				dir.mkdirs();
			else
				throw new JDOException("FileBucket "+name+" not found");
		else if (!dir.isDirectory())
			throw new JDOException("File " + path+name + " already exists but it is not a directory");
		else if (!dir.canRead())
			throw new JDOException("FileBucket can't read from " + path);
		else if (!dir.canWrite())
			throw new JDOException("FileBucket can't write to " + path);
	}

	@Override
	public String name() {
		return dir.getName();
	}

	private String getFilePath(final Object key) throws NullPointerException {
		if (null==key)
			throw new NullPointerException("FileBucket file name cannot be null");
		if (key instanceof Param)
			return path + ((Param) key).getValue();
		else
			return path + key;
	}

	private String getFileExtension(final String fileName) throws NullPointerException {
		final int lastDot = fileName.lastIndexOf('.');
		return lastDot>=0 ? fileName.substring(lastDot) : "";
	}

	@Override
	public boolean exists(Object key) throws JDOException {
		return new File(getFilePath(key)).exists();
	}

	@Override
	public boolean load(Object key, Stored target) throws JDOException {
		final File source = new File(getFilePath(key));
		final boolean retval = source.exists();
		if (!source.isFile())
			throw new JDOException("FileBucket.load() " + source.getAbsolutePath() + " is not a file");
		try {
			if (retval)
				target.setContent(FileUtils.readFileToByteArray(source), MimeUtils.getFileExtensionForMimeType(getFileExtension(source.getName())));
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
