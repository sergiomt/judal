package org.judal.jdbc;

import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

import javax.jdo.JDOException;

import org.judal.jdbc.metadata.SQLTableDef;
import org.judal.jdbc.metadata.SQLViewDef;
import org.judal.metadata.MetadataObjectFactory;
import org.judal.metadata.ProcedureDef;
import org.judal.metadata.SequenceDef;
import org.judal.metadata.TableDef;
import org.judal.metadata.TriggerDef;
import org.judal.metadata.ViewDef;

public class JDBCMetadataObjectFactory extends MetadataObjectFactory {

	private static final Set<Class<?>> EMPTY_SET = Collections.unmodifiableSet(new HashSet<Class<?>>());
	
	private static Set<Class<?>> getObjectDefSubclasses(RDBMS dbms, Class<?> objectClass) throws NoSuchMethodException, JDOException {
		final String packageName = "org.judal.jdbc."+dbms.shortName();
		try {
			Class.forName(packageName+".Loader");
		} catch (Exception ignore) { }
		Package pkg = Package.getPackage(packageName);
		if (pkg==null)
			return EMPTY_SET;
		else
			return getSubclassesOf(pkg, objectClass);
	}

	@SuppressWarnings("unchecked")
	public static <T extends ViewDef> T newViewDef(RDBMS dbms, Object... constructorParameters) throws NoSuchMethodException, JDOException {
		Set<Class<?>> subclasses = getObjectDefSubclasses(dbms, ViewDef.class);
		return newViewDef((Class<T>) (subclasses.isEmpty() ? SQLViewDef.class : subclasses.iterator().next()), constructorParameters);
	}

	@SuppressWarnings("unchecked")
	public static <T extends TableDef> T newTableDef(RDBMS dbms, Object... constructorParameters) throws NoSuchMethodException, JDOException {
		Set<Class<?>> subclasses = getObjectDefSubclasses(dbms, TableDef.class);
		return newTableDef((Class<T>) (subclasses.isEmpty() ? SQLTableDef.class : subclasses.iterator().next()), constructorParameters);
	}

	@SuppressWarnings("unchecked")
	public static <T extends ProcedureDef> T newProcedureDef(RDBMS dbms, Object... constructorParameters) throws NoSuchMethodException, JDOException {
		Set<Class<?>> subclasses = getObjectDefSubclasses(dbms, ProcedureDef.class);
		return newProcedureDef((Class<T>) (subclasses.isEmpty() ? ProcedureDef.class : subclasses.iterator().next()), constructorParameters);
	}

	@SuppressWarnings("unchecked")
	public static <T extends TriggerDef> T newTriggerDef(RDBMS dbms, Object... constructorParameters) throws NoSuchMethodException, JDOException {
		Set<Class<?>> subclasses = getObjectDefSubclasses(dbms, TriggerDef.class);
		return newTriggerDef((Class<T>) (subclasses.isEmpty() ? TriggerDef.class : subclasses.iterator().next()), constructorParameters);
	}

	@SuppressWarnings("unchecked")
	public static <T extends SequenceDef> T newSequenceDef(RDBMS dbms, Object... constructorParameters) throws NoSuchMethodException, JDOException {
		Set<Class<?>> subclasses = getObjectDefSubclasses(dbms, SequenceDef.class);
		return newSequenceDef((Class<T>) (subclasses.isEmpty() ? SequenceDef.class : subclasses.iterator().next()), constructorParameters);
	}

}
