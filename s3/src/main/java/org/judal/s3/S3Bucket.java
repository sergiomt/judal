package org.judal.s3;

/*
 * © Copyright 2016 the original author.
 * This file is licensed under the Apache License version 2.0.
 * You may not use this file except in compliance with the license.
 * You may obtain a copy of the License at:
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.
 */

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;

import com.amazonaws.AmazonClientException;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectInputStream;
import com.knowgate.debug.DebugFile;
import com.knowgate.io.StreamPipe;

import org.judal.storage.Param;

import javax.jdo.FetchPlan;
import javax.jdo.JDOException;
import javax.jdo.PersistenceManager;

import org.judal.storage.keyvalue.Bucket;
import org.judal.storage.keyvalue.Stored;

/**
 * Implementation of Bucket interface for Amazon S3
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class S3Bucket implements Bucket {

	private String bucketName;
	private S3DataSource s3dts;
	protected Class<? extends Stored> candidateClass;
	protected Collection<S3Iterator> iterators;

	public S3Bucket(S3DataSource oDts, String sBucketName) {
		bucketName = sBucketName;
		s3dts = oDts;
	}

	/**
	 * @return AmazonS3Client
	 */
	public AmazonS3Client getClient() {
		return s3dts.getClient();
	}

	/**
	 * @return String Bucket Name
	 */
	@Override
	public String name() {
		return bucketName;
	}

	@Override
	public void close() throws JDOException {
	}

	/**
	 * Check whether an object with the given key is at this Bucket
	 * @param key Param or String value
	 * @return boolean
	 * @throws NullPointerException if key is <b>null</b>
	 * @throws IllegalArgumentException
	 * @throws JDOException
	 */
	@Override
	public boolean exists(Object key) throws NullPointerException, IllegalArgumentException, JDOException {
		if (null==key) throw new NullPointerException("S3Bucket.exists() key value cannot be null");

		Object value;
		if (key instanceof Param)
			value = ((Param) key).getValue();
		else
			value = key;

		if (DebugFile.trace) {
			DebugFile.writeln("Begin S3Bucket.exists("+value+")");
			DebugFile.incIdent();
		}
		S3Object s3obj = null;
		try {
			s3obj = getClient().getObject(name(), value.toString());
			s3obj.close();
		} catch (IOException ioe) {
			throw new JDOException(ioe.getMessage(), ioe);
		} catch (AmazonS3Exception as3e) {
		} finally {
			try { if (s3obj!=null) s3obj.close(); } catch (IOException ignore) {
				if (DebugFile.trace) DebugFile.writeln("S3Record.load("+value+") IOException "+ignore.getMessage());
			}			
		}
		if (DebugFile.trace) {
			DebugFile.decIdent();
			DebugFile.writeln("End S3Bucket.exists() : "+String.valueOf(s3obj!=null));
		}
		return s3obj!=null;
	}

	/**
	 * <p>Load an object and its metadata (if present) from Bucket.</p>
	 * Default metadata properties loaded into S3Record are: cacheControl, contentDisposition, contentEncoding, contentLength, contentMD5, contentType, ETag, expirationTime, expirationTimeRuleId, httpExpiresDate, lastModified, restoreExpirationTime, serverSideEncryption, versionId
	 * @param key String
	 * @param target S3Record
	 * @return boolean <b>true</b>if an object with the given key was loaded <b>false</b> otherwise
	 */
	@Override
	public boolean load(Object key, Stored target) throws NullPointerException, JDOException {
		if (null==key) throw new NullPointerException("S3Bucket.load() key value cannot be null");

		String value;
		if (key instanceof Param)
			value = ((Param) key).getValue().toString();
		else
			value = key.toString();

		boolean retval = true;
		S3Record s3rec = (S3Record) target;
		S3Object s3obj = null;
		ObjectMetadata s3objmetadata = null;
		ByteArrayOutputStream outstrm = null;
		S3ObjectInputStream s3instrm = null;

		try {
			if (DebugFile.trace) DebugFile.writeln("AmazonS3Client.getObject("+name()+","+value+")");
			s3obj = getClient().getObject(name(), value);
			if (DebugFile.trace) DebugFile.writeln("retrived object is "+(s3obj!=null ? "not ": "")+"null");
			if (s3obj!=null) {
				s3objmetadata = s3obj.getObjectMetadata();
				s3rec.setKey(value);
				if (s3objmetadata==null) {
					if (DebugFile.trace) DebugFile.writeln("retrived object "+name()+" has no metadata");
					outstrm = new ByteArrayOutputStream();
				} else {
					if (DebugFile.trace) DebugFile.writeln("retrived object "+name()+" metadata content length is "+String.valueOf(s3objmetadata.getContentLength()));
					if (s3objmetadata.getContentLength()>0l)
						outstrm = new ByteArrayOutputStream((int) s3objmetadata.getContentLength());
					else
						outstrm = new ByteArrayOutputStream();
				}
				try {
					s3instrm = s3obj.getObjectContent();
					new StreamPipe().between(s3instrm, outstrm);
					s3rec.setContent(outstrm.toByteArray(), s3objmetadata.getContentType());
					outstrm.close();
					outstrm = null;
					s3instrm.close();
					s3instrm = null;
				} catch (IOException ioe) {
					throw new JDOException(ioe.getMessage(), ioe);
				}
				if (s3objmetadata!=null) {
					s3rec.put("cacheControl", s3objmetadata.getCacheControl());
					s3rec.put("contentDisposition", s3objmetadata.getContentDisposition());
					s3rec.put("contentEncoding", s3objmetadata.getContentEncoding());
					s3rec.put("contentLength", new Long(s3objmetadata.getContentLength()));
					s3rec.put("contentMD5", s3objmetadata.getContentMD5());				
					s3rec.put("contentType", s3objmetadata.getContentType());
					s3rec.put("ETag", s3objmetadata.getETag());
					s3rec.put("expirationTime", s3objmetadata.getExpirationTime());
					s3rec.put("expirationTimeRuleId", s3objmetadata.getExpirationTimeRuleId());
					s3rec.put("httpExpiresDate", s3objmetadata.getHttpExpiresDate());
					s3rec.put("lastModified", s3objmetadata.getLastModified());
					s3rec.put("restoreExpirationTime", s3objmetadata.getRestoreExpirationTime());
					s3rec.put("serverSideEncryption", s3objmetadata.getServerSideEncryption());
					s3rec.put("versionId", s3objmetadata.getVersionId());
					Map<String,String> s3umd = s3objmetadata.getUserMetadata();
					if (s3umd!=null)
						for (Map.Entry<String,String> e : s3umd.entrySet())
							s3rec.put(e.getKey(), e.getValue());
				}
			}
			s3obj.close();
			s3obj = null;
		} catch (IOException ioe) {
			throw new JDOException(ioe.getMessage(), ioe);
		} catch (AmazonS3Exception s3e) {
			if (s3e.getErrorCode().equals("NoSuchKey"))
				retval = false;
			else
				throw new JDOException(s3e.getMessage(), s3e);
		} catch (AmazonClientException ace) {
			throw new JDOException(ace.getMessage(), ace);
		} finally {
			try { if (outstrm!=null) outstrm.close(); } catch (IOException ignore) {
				if (DebugFile.trace) DebugFile.writeln("S3Record.load("+key+") IOException "+ignore.getMessage());
			}
			try { if (s3instrm!=null) s3instrm.close(); } catch (IOException ignore) {
				if (DebugFile.trace) DebugFile.writeln("S3Record.load("+key+") IOException "+ignore.getMessage());
			}
			try { if (s3obj!=null) s3obj.close(); } catch (IOException ignore) {
				if (DebugFile.trace) DebugFile.writeln("S3Record.load("+key+") IOException "+ignore.getMessage());
			}
		}
		return retval;
	}

	/**
	 * @param record S3Record
	 */
	@Override
	public void store(Stored record) throws JDOException {
		S3Record s3rec = (S3Record) record;
		ByteArrayInputStream oIn = new ByteArrayInputStream(s3rec.getValue());
		try {
			getClient().putObject(name(), s3rec.getKey(), oIn, s3rec.getMetadata());
		} catch (AmazonS3Exception s3e) {
			throw new JDOException("AmazonS3Exception at S3Bucket.store(Stored) "+s3e.getMessage(), s3e);
		}
	}

	/**
	 * @param key Param or String
	 */
	@Override
	public void delete(Object key) throws NullPointerException, IllegalArgumentException, JDOException {
		String keyval;
		if (key instanceof Param)
			keyval = ((Param) key).getValue().toString();
		else if (key instanceof String)
			keyval = (String) key;
		else 
			keyval = key.toString();
		try {
			getClient().deleteObject(name(), keyval);
		} catch (AmazonS3Exception s3e) {
			throw new JDOException("AmazonS3Exception at S3Bucket.delete(Object) "+s3e.getMessage(), s3e);
		}
	}

	/**
	 * @param candidateClass Class&lt;Stored&gt;
	 */
	@Override
	public void setClass(Class<? extends Stored> candidateClass) {
		this.candidateClass = candidateClass;
	}

	@Override
	public void close(Iterator<Stored> notused) { }

	@Override
	public void closeAll() { }

	@Override
	public Class<Stored> getCandidateClass() {
		return (Class<Stored>) candidateClass;
	}

	/**
	 * @return This method always returns <b>null</b>
	 */
	@Override
	public FetchPlan getFetchPlan() {
		return null;
	}

	/**
	 * @return This method always returns <b>null</b>
	 */
	@Override
	public PersistenceManager getPersistenceManager() {
		return null;
	}

	/**
	 * @return This method always returns <b>false</b>
	 */
	@Override
	public boolean hasSubclasses() {
		return false;
	}

	/**
	 * @return S3Iterator Over all the values stored at this Bucket
	 */
	@Override
	public Iterator<Stored> iterator() {
		Iterator<Stored> retval;
		if (null==iterators)
			iterators = new LinkedList<S3Iterator>();
		try {
			retval = new S3Iterator(S3Bucket.class, this, null);
		} catch (NoSuchMethodException | SecurityException xcpt) {
			throw new JDOException(xcpt.getMessage(), xcpt);
		}
		return retval;
	}

}
