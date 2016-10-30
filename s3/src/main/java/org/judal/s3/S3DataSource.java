package org.judal.s3;

/**
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

import java.util.HashMap;
import java.util.Iterator;
import java.util.Map;

import javax.transaction.TransactionManager;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.s3.model.Region;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ListVersionsRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.s3.model.S3VersionSummary;
import com.amazonaws.services.s3.model.VersionListing;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Regions;

import com.knowgate.debug.DebugFile;

import javax.jdo.JDOException;
import javax.jdo.JDOUnsupportedOptionException;
import javax.jdo.datastore.JDOConnection;
import javax.jdo.datastore.Sequence;

import org.judal.metadata.SchemaMetaData;
import org.judal.storage.Bucket;
import org.judal.storage.BucketDataSource;
import org.judal.storage.DataSource;
import org.judal.storage.Param;

/**
 * Implementation of BucketDataSource forAmazon S3
 * @author Sergio Montoro Ten
 * @version 1.0
 */
public class S3DataSource implements BucketDataSource {

	private AmazonS3Client as3cli;
	private SchemaMetaData smd;
	private Map<String,String> props;

	/**
	 * 
	 * @param properties Must contain: region, accessKey, secretKey
	 * @param transactManager
	 */
	public S3DataSource(Map<String,String> properties) {
		props = new HashMap<String,String>(17);
		props.putAll(properties);
		as3cli = new AmazonS3Client(new BasicAWSCredentials(props.get(DataSource.USER),props.get(DataSource.PASSWORD)));
		as3cli.setRegion(com.amazonaws.regions.Region.getRegion(Regions.fromName(props.get(DataSource.REGION))));
	}

	/**
	 * Check whether a Bucket with the given name exists
	 * @param objectName String Bucket Name
	 * @param objectType String Must have value "U"
	 * @return boolean
	 */
	@Override
	public boolean exists(String objectName, String objectType) throws JDOException {
		boolean retval;
		if (objectType.equals("U")) {
			retval = as3cli.doesBucketExist(objectName);
		} else {
			retval = false;
		}
		return retval;
	}

	/**
	 * <p>Covert specified at region property into a Region object instance according to the following mapping:</p> 
	 * "us-east-1" => US_Standard, "us-west-1" => US_West, "us-west-2" => US_West_2, "eu-west-1" => EU_Ireland, "ap-southeast-1" => AP_Singapore, "ap-southeast-2" => AP_Sydney, "ap-northeast-1" => AP_Tokyo, "sa-east-1" => SA_SaoPaulo
	 * @return Region
	 * @throws JDOException If the region set is none of the above
	 */
	public Region getRegion() throws JDOException {
		final String regionName = props.get(DataSource.REGION).toLowerCase();
		if (regionName.equals("us-east-1"))
			return Region.US_Standard;
		else if (regionName.equals("us-west-1"))
			return Region.US_West;
		else if (regionName.equals("us-west-2"))
			return Region.US_West_2;
		else if (regionName.equals("eu-west-1"))
			return Region.EU_Ireland;
		else if (regionName.equals("ap-southeast-1"))
			return Region.AP_Singapore;
		else if (regionName.equals("ap-southeast-2"))
			return Region.AP_Sydney;
		else if (regionName.equals("ap-northeast-1"))
			return Region.AP_Tokyo;
		else if (regionName.equals("sa-east-1"))
			return Region.SA_SaoPaulo;
		throw new JDOException("Unrecognized S3 region name "+regionName);
	}

	/**
	 * @return AmazonS3Client
	 */
	public AmazonS3Client getClient() {
		return as3cli;
	}

	/**
	 * return Map&lt;String,String&gt;
	 */
	@Override
	public Map<String, String> getProperties() {
		return props;
	}

	/**
	 * S3 does not support transactions. This method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public TransactionManager getTransactionManager() {
		throw new JDOUnsupportedOptionException("Amazon S3 does not support transactions");
	}

	/**
	 * S3 does not support transactions. This method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public boolean inTransaction() throws JDOException {
		throw new JDOUnsupportedOptionException("Amazon S3 does not support transactions");
	}

	@Override
	public void close() throws JDOException {
	}

	public SchemaMetaData getMetaData() throws JDOException, UnsupportedOperationException {
		return smd;
	}

	public void setMetaData(SchemaMetaData oSmd) throws JDOException,UnsupportedOperationException {
		this.smd = oSmd;
	}

	/**
	 * Create a Bucket the region of this S3DataSource.
	 * If a Bucket with the given name already exists then nothing is done.
	 * @param bucketName String
	 * @param options Map&lt;String,Object&gt; Unused
	 */
	@Override
	public void createBucket(String bucketName, Map<String,Object> options) throws JDOException {
		try {
			if (!as3cli.doesBucketExist(bucketName))
				as3cli.createBucket(bucketName,getRegion());
		} catch (AmazonClientException ace) {
			throw new JDOException(ace.getMessage(),ace);
		}
	}

	/**
	 * @param bucketName String
	 * @return S3Bucket
	 */
	@Override
	public Bucket openBucket(String bucketName) throws JDOException {

		if (DebugFile.trace) DebugFile.writeln("Begin S3DataSource.openBucket("+bucketName+")");

		S3Bucket oRetVal = new S3Bucket(this, bucketName);

		if (DebugFile.trace) DebugFile.writeln("End S3DataSource.openBucket("+bucketName+") : "+oRetVal);

		return oRetVal;
	}

	/**
	 * @param bucketName String
	 */
	@Override
	public void dropBucket(String bucketName) throws JDOException {
		as3cli.deleteBucket(bucketName);	
	}

	/**
	 * Delete all objects at this Bucket including all their versions.
	 * @param bucketName String
	 * @throws JDOException
	 */
	@Override
	public void truncateBucket(String bucketName) throws JDOException {
		try {
			ObjectListing objectListing = as3cli.listObjects(bucketName);
			while (true) {
				for ( Iterator<?> iterator = objectListing.getObjectSummaries().iterator(); iterator.hasNext(); )
					as3cli.deleteObject(bucketName, ((S3ObjectSummary) iterator.next()).getKey());
				if (objectListing.isTruncated())
					objectListing = as3cli.listNextBatchOfObjects(objectListing);
				else
					break;
			}
			VersionListing list = as3cli.listVersions(new ListVersionsRequest().withBucketName(bucketName));
			for ( Iterator<?> iterator = list.getVersionSummaries().iterator(); iterator.hasNext(); ) {
				S3VersionSummary s = (S3VersionSummary)iterator.next();
				as3cli.deleteVersion(bucketName, s.getKey(), s.getVersionId());
			}
		} catch (AmazonServiceException ase) {
			throw new JDOException("AmazonServiceException status "+ase.getStatusCode()+" error code "+ase.getErrorCode()+" type "+ase.getErrorType()+" request id "+ase.getRequestId()+" "+ase.getMessage(), ase);
		} catch (AmazonClientException ace) {
			throw new JDOException("AmazonClientException "+ace.getMessage(), ace);
		}
	}

	/**
	 * S3 does not use connections. This method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public JDOConnection getJdoConnection() throws JDOException {
		throw new JDOUnsupportedOptionException("Amazon S3 does not use connections");
	}

	/**
	 * S3 does not implement sequences. This method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public Sequence getSequence(String name) throws JDOException {
		throw new JDOUnsupportedOptionException("Amazon S3 does not provide sequences");
	}

	/**
	 * S3 does not provide callable statements. This method will always raise JDOUnsupportedOptionException
	 * @throws JDOUnsupportedOptionException
	 */
	@Override
	public Object call(String statement, Param... parameters) throws JDOException {
		throw new JDOUnsupportedOptionException("Amazon S3 does not support callable statements");
	}		

}
