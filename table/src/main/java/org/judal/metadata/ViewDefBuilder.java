package org.judal.metadata;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;

import java.util.Deque;
import java.util.List;
import java.util.ArrayDeque;
import java.util.ArrayList;

import org.judal.storage.table.Record;

public class ViewDefBuilder {

    private ViewDefBuilder() { }

    /**
     * <p>Create a ViewDef for a class which implements Record interface</p>
     * @param recClass Class&ldquo;? extends Record&rdquo; Class implementing Record interface
     * @param tableName String
     */
    public static ViewDef of(Class<? extends Record> recClass, String tableName) {
        final Deque<Class<?>> superClasses = new ArrayDeque<>();
        final List<ColumnDef> columns = new ArrayList<>();
        superClasses.add(recClass);
        for (Class<?> superClass = recClass.getSuperclass(); superClass!=null; superClass = superClass.getSuperclass()) {
            superClasses.add(superClass);
        }
        superClasses.forEach(clazz ->  addColumns(clazz, columns));
        return new ViewDef(tableName, columns.toArray(new ColumnDef[columns.size()])); // NOSONAR
    }

    private static void addColumns(Class<?> clazz, List<ColumnDef> columns) {
        for (Field f : clazz.getDeclaredFields()) {
            f.setAccessible(true); // NOSONAR
            if ((f.getModifiers() & Modifier.TRANSIENT) != 0) {
                columns.add(new ColumnDef(f.getName(),ColumnDef.typeForClass(f.getClass()), columns.size() + 1));
            }
        }
    }
}
