 package sqlitetools;


import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.base.CharMatcher;
import com.google.common.base.Joiner;
import com.google.common.base.Strings;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;

/**
 *  1、 getXXX_1与getXXX_2的区别只在返回值的类型不同，处理过程是一样的。
 *  2、 查找（select）时，如果调用的名字不是叫【XXXByColumnNames】，那么基本上就是【select *】 了
 *  3、getXXX_2 与getXXX_2_2的区别是，前者返回Map<String, String>，后者返回Map<String, Object>。这个方要是由于java泛型造成的不便。。
 *  （但是其实上，值是没有变化的）
 */
public final class SqliteTools {
	 
	static {
		try {
			Class.forName("org.sqlite.JDBC");
		} catch (Exception e) {
			throw new IllegalStateException("sqlite驱动包没有加");
		}
	}
	
	public static void main(String... args){
		final String os = (String)System.getProperties().get("os.name");
		System.out.println(os);
	}
	
	
	
	public static List<String> getOne_1(final Connection conn, final String tableName, final String where, final String orderBy, final Long offset){
		final StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("select * from `").append(tableName).append("` ");
		if(!Strings.isNullOrEmpty(where)){
			sqlBuilder.append(" where ")/*.append(" where 1=1 ")*/.append(where).append(" ");
		}
		if(!Strings.isNullOrEmpty(orderBy)){
			sqlBuilder.append(" ").append(orderBy).append(" ");
		}
		if(offset != null && offset >= 0){
			sqlBuilder.append(" limit 1 ").append(" offset ").append(offset).append(" ");
		}
		final String sql = sqlBuilder.toString();
		try (final Statement st = conn.createStatement();final ResultSet rs = st.executeQuery(sql)){
			final ResultSetMetaData meta = rs.getMetaData();
			final int columnCount = meta.getColumnCount();
			if(rs.next()){
				final List<String> row = Lists.newArrayList();
				for(int i=1; i<=columnCount; i++){
					row.add(rs.getString(i));
				}
				return row;
			}
			return null;
		}catch(Exception e){
			throw new IllegalArgumentException("查询sqlite失败", e);
		}
	}
	
	static String columnNamesStr(final String[] columnNames){
		return Joiner.on(",").join(new Iterator<String>(){
			int index=0;
			@Override
			public boolean hasNext() {
				return index<columnNames.length;
			}
			@Override
			public String next() {
				final String columnName = columnNames[index++];
				final CharMatcher cm1 = CharMatcher.JAVA_LETTER.or(CharMatcher.JAVA_DIGIT).or(CharMatcher.is('_'));
				final CharMatcher cm2 = CharMatcher.JAVA_LETTER.or(CharMatcher.anyOf("_"));
				if(cm2.matches(columnName.charAt(0)) && 
						((columnName.length() == 1) || (columnName.length() > 1 && cm1.matchesAllOf(columnName.substring(1))))
						){
					return "`"+ columnName+ "`";
				}else{
					return " "+ columnName +" ";
				}
				
			}
		});
	}
	
	public static List<String> getOneByColumnNames_1(final Connection conn, final String tableName,final String[] columnNames, final String where, final String orderBy, final Long offset){
		if(columnNames == null || columnNames.length <= 0){
			throw new IllegalArgumentException();
		}
		
		final StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("select ").append(columnNamesStr(columnNames)).append(" from `").append(tableName).append("` ");
		if(!Strings.isNullOrEmpty(where)){
			sqlBuilder.append(" where ")/*.append(" where 1=1 ")*/.append(where).append(" ");
		}
		if(!Strings.isNullOrEmpty(orderBy)){
			sqlBuilder.append(" ").append(orderBy).append(" ");
		}
		if(offset != null && offset >= 0){
			sqlBuilder.append(" limit 1 ").append(" offset ").append(offset).append(" ");
		}
		final String sql = sqlBuilder.toString();
		try (final Statement st = conn.createStatement();final ResultSet rs = st.executeQuery(sql)){
//			final ResultSetMetaData meta = rs.getMetaData();
//			final int columnCount = meta.getColumnCount();
			if(rs.next()){
				final List<String> row = Lists.newArrayList();
				for(int i=1; i<=columnNames.length; i++){
					row.add(rs.getString(i));//这里不能用rs.getString(【columnNames[i]】)
				}
				return row;
			}
			return null;
		}catch(Exception e){
			throw new IllegalArgumentException("查询sqlite失败", e);
		}
	}
	
	
	public static Map<String, String> getOne_2(final Connection conn, final String tableName, final String where, final String orderBy, final Long offset){
		final StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("select * from `").append(tableName).append("` ");
		if(!Strings.isNullOrEmpty(where)){
			sqlBuilder.append(" where ")/*.append(" where 1=1 ")*/.append(where).append(" ");
		}
		if(!Strings.isNullOrEmpty(orderBy)){
			sqlBuilder.append(" ").append(orderBy).append(" ");
		}
		if(offset != null && offset >= 0){
			sqlBuilder.append(" limit 1 ").append(" offset ").append(offset).append(" ");
		}
		final String sql = sqlBuilder.toString();
		try (final Statement st = conn.createStatement();final ResultSet rs = st.executeQuery(sql)){
			final ResultSetMetaData meta = rs.getMetaData();
			final int columnCount = meta.getColumnCount();
			while(rs.next()){
				final Map<String, String> row = Maps.newHashMap();
				for(int i=1; i<=columnCount; i++){
					row.put(meta.getColumnName(i), rs.getString(i));
				}
				return row;
			}
			return null;
		}catch(Exception e){
			throw new IllegalArgumentException("查询sqlite失败", e);
		}
	}
	
	
	public static Map<String, Object> getOne_2_2(final Connection conn, final String tableName, final String where, final String orderBy, final Long offset){
		final StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("select * from `").append(tableName).append("` ");
		if(!Strings.isNullOrEmpty(where)){
			sqlBuilder.append(" where ")/*.append(" where 1=1 ")*/.append(where).append(" ");
		}
		if(!Strings.isNullOrEmpty(orderBy)){
			sqlBuilder.append(" ").append(orderBy).append(" ");
		}
		if(offset != null && offset >= 0){
			sqlBuilder.append(" limit 1 ").append(" offset ").append(offset).append(" ");
		}
		final String sql = sqlBuilder.toString();
		try (final Statement st = conn.createStatement();final ResultSet rs = st.executeQuery(sql)){
			final ResultSetMetaData meta = rs.getMetaData();
			final int columnCount = meta.getColumnCount();
			while(rs.next()){
				final Map<String, Object> row = Maps.newHashMap();
				for(int i=1; i<=columnCount; i++){
					row.put(meta.getColumnName(i), rs.getString(i));
				}
				return row;
			}
			return null;
		}catch(Exception e){
			throw new IllegalArgumentException("查询sqlite失败", e);
		}
	}
	
	
	public static Map<String, String> getOneByColumnNames_2(final Connection conn, final String tableName,final String[] columnNames , final String where, final String orderBy, final Long offset){
		if(columnNames == null || columnNames.length <= 0){
			throw new IllegalArgumentException();
		}
		final StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("select ").append(columnNamesStr(columnNames)).append(" from `").append(tableName).append("` ");
		if(!Strings.isNullOrEmpty(where)){
			sqlBuilder.append(" where ")/*.append(" where 1=1 ")*/.append(where).append(" ");
		}
		if(!Strings.isNullOrEmpty(orderBy)){
			sqlBuilder.append(" ").append(orderBy).append(" ");
		}
		if(offset != null && offset >= 0){
			sqlBuilder.append(" limit 1 ").append(" offset ").append(offset).append(" ");
		}
		final String sql = sqlBuilder.toString();
		try (final Statement st = conn.createStatement();final ResultSet rs = st.executeQuery(sql)){
//			final ResultSetMetaData meta = rs.getMetaData();
//			final int columnCount = meta.getColumnCount();
			while(rs.next()){
				final Map<String, String> row = Maps.newHashMap();
				for(int i=1; i<=columnNames.length; i++){
					row.put(columnNames[i-1], rs.getString(i));//这里不能用rs.getString(【columnNames[i]】)
				}
				return row;
			}
			return null;
		}catch(Exception e){
			throw new IllegalArgumentException("查询sqlite失败", e);
		}
	}
	
	
	public static Map<String, Object> getOneByColumnNames_2_2(final Connection conn, final String tableName,final String[] columnNames , final String where, final String orderBy, final Long offset){
		if(columnNames == null || columnNames.length <= 0){
			throw new IllegalArgumentException();
		}
		final StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("select ").append(columnNamesStr(columnNames)).append(" from `").append(tableName).append("` ");
		if(!Strings.isNullOrEmpty(where)){
			sqlBuilder.append(" where ")/*.append(" where 1=1 ")*/.append(where).append(" ");
		}
		if(!Strings.isNullOrEmpty(orderBy)){
			sqlBuilder.append(" ").append(orderBy).append(" ");
		}
		if(offset != null && offset >= 0){
			sqlBuilder.append(" limit 1 ").append(" offset ").append(offset).append(" ");
		}
		final String sql = sqlBuilder.toString();
		try (final Statement st = conn.createStatement();final ResultSet rs = st.executeQuery(sql)){
//			final ResultSetMetaData meta = rs.getMetaData();
//			final int columnCount = meta.getColumnCount();
			while(rs.next()){
				final Map<String, Object> row = Maps.newHashMap();
				for(int i=1; i<=columnNames.length; i++){
					row.put(columnNames[i-1], rs.getString(i));//这里不能用rs.getString(【columnNames[i]】)
				}
				return row;
			}
			return null;
		}catch(Exception e){
			throw new IllegalArgumentException("查询sqlite失败", e);
		}
	}
	
	
	
	public static List<List<String>> getAll_1(final Connection conn, final String tableName, final String where, final String orderBy, final Long offset, final Long limit){
		final StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("select * from `").append(tableName).append("` ");
		if(!Strings.isNullOrEmpty(where)){
			sqlBuilder.append(" where ")/*.append(" where 1=1 ")*/.append(where).append(" ");
		}
		if(!Strings.isNullOrEmpty(orderBy)){
			sqlBuilder.append(" ").append(orderBy).append(" ");
		}
		if(limit != null && limit >= 0){
			sqlBuilder.append(" limit ").append(limit).append(" ");
		}
		if(offset != null && offset >= 0){
			sqlBuilder.append(" offset ").append(offset).append(" ");
		}
		final String sql = sqlBuilder.toString();
		final List<List<String>> r = Lists.newArrayList();
		try (final Statement st = conn.createStatement();final ResultSet rs = st.executeQuery(sql)){
			final ResultSetMetaData meta = rs.getMetaData();
			final int columnCount = meta.getColumnCount();
			while(rs.next()){
				final List<String> row = Lists.newArrayList();
				for(int i=1; i<=columnCount; i++){
					row.add(rs.getString(i));
				}
				r.add(row);
			}
			return r;
		}catch(Exception e){
			throw new IllegalArgumentException("查询sqlite失败", e);
		}
	}
	
	
	public static List<List<String>> getAllByColumnNames_1(final Connection conn, final String tableName,final String[] columnNames , final String where, final String orderBy, final Long offset, final Long limit){
		if(columnNames == null || columnNames.length <= 0){
			throw new IllegalArgumentException();
		}
		final StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("select ").append(columnNamesStr(columnNames)).append(" from `").append(tableName).append("` ");
		if(!Strings.isNullOrEmpty(where)){
			sqlBuilder.append(" where ")/*.append(" where 1=1 ")*/.append(where).append(" ");
		}
		if(!Strings.isNullOrEmpty(orderBy)){
			sqlBuilder.append(" ").append(orderBy).append(" ");
		}
		if(limit != null && limit >= 0){
			sqlBuilder.append(" limit ").append(limit).append(" ");
		}
		if(offset != null && offset >= 0){
			sqlBuilder.append(" offset ").append(offset).append(" ");
		}
		final String sql = sqlBuilder.toString();
		final List<List<String>> r = Lists.newArrayList();
		try (final Statement st = conn.createStatement();final ResultSet rs = st.executeQuery(sql)){
//			final ResultSetMetaData meta = rs.getMetaData();
//			final int columnCount = meta.getColumnCount();
			while(rs.next()){
				final List<String> row = Lists.newArrayList();
				for(int i=1; i<=columnNames.length; i++){
					row.add(rs.getString(i));//这里不能用rs.getString(【columnNames[i]】)
				}
				r.add(row);
			}
			return r;
		}catch(Exception e){
			throw new IllegalArgumentException("查询sqlite失败", e);
		}
	}
	
	public static List<Map<String, String>> getAll_2(final Connection conn, final String tableName, final String where, final String orderBy, final Long offset, final Long limit){
		final StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("select * from `").append(tableName).append("` ");
		if(!Strings.isNullOrEmpty(where)){
			sqlBuilder.append(" where ")/*.append(" where 1=1 ")*/.append(where).append(" ");
		}
		if(!Strings.isNullOrEmpty(orderBy)){
			sqlBuilder.append(" ").append(orderBy).append(" ");
		}
		if(limit != null && limit >= 0){
			sqlBuilder.append(" limit ").append(limit).append(" ");
		}
		if(offset != null && offset >= 0){
			sqlBuilder.append(" offset ").append(offset).append(" ");
		}
		final String sql = sqlBuilder.toString();
		final List<Map<String, String>> r = Lists.newArrayList();
		try (final Statement st = conn.createStatement();final ResultSet rs = st.executeQuery(sql)){
			final ResultSetMetaData meta = rs.getMetaData();
			final int columnCount = meta.getColumnCount();
			while(rs.next()){
				final Map<String, String> row = Maps.newHashMap();
				for(int i=1; i<=columnCount; i++){
					row.put(meta.getColumnName(i), rs.getString(i));
				}
				r.add(row);
			}
			return r;
		}catch(Exception e){
			throw new IllegalArgumentException("查询sqlite失败", e);
		}
	}
	
	public static List<Map<String, Object>> getAll_2_2(final Connection conn, final String tableName, final String where, final String orderBy, final Long offset, final Long limit){
		final StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("select * from `").append(tableName).append("` ");
		if(!Strings.isNullOrEmpty(where)){
			sqlBuilder.append(" where ")/*.append(" where 1=1 ")*/.append(where).append(" ");
		}
		if(!Strings.isNullOrEmpty(orderBy)){
			sqlBuilder.append(" ").append(orderBy).append(" ");
		}
		if(limit != null && limit >= 0){
			sqlBuilder.append(" limit ").append(limit).append(" ");
		}
		if(offset != null && offset >= 0){
			sqlBuilder.append(" offset ").append(offset).append(" ");
		}
		final String sql = sqlBuilder.toString();
		final List<Map<String, Object>> r = Lists.newArrayList();
		try (final Statement st = conn.createStatement();final ResultSet rs = st.executeQuery(sql)){
			final ResultSetMetaData meta = rs.getMetaData();
			final int columnCount = meta.getColumnCount();
			while(rs.next()){
				final Map<String, Object> row = Maps.newHashMap();
				for(int i=1; i<=columnCount; i++){
					row.put(meta.getColumnName(i), rs.getString(i));
				}
				r.add(row);
			}
			return r;
		}catch(Exception e){
			throw new IllegalArgumentException("查询sqlite失败", e);
		}
	}
	
	
	
	public static List<Map<String, String>> getAllByColumnNames_2(final Connection conn, final String tableName, final String[] columnNames , final String where, final String orderBy, final Long offset, final Long limit){
		if(columnNames == null || columnNames.length <= 0){
			throw new IllegalArgumentException();
		}
		final StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("select ").append(columnNamesStr(columnNames)).append(" from `").append(tableName).append("` ");
		if(!Strings.isNullOrEmpty(where)){
			sqlBuilder.append(" where ")/*.append(" where 1=1 ")*/.append(where).append(" ");
		}
		if(!Strings.isNullOrEmpty(orderBy)){
			sqlBuilder.append(" ").append(orderBy).append(" ");
		}
		if(limit != null && limit >= 0){
			sqlBuilder.append(" limit ").append(limit).append(" ");
		}
		if(offset != null && offset >= 0){
			sqlBuilder.append(" offset ").append(offset).append(" ");
		}
		final String sql = sqlBuilder.toString();
		final List<Map<String, String>> r = Lists.newArrayList();
		try (final Statement st = conn.createStatement();final ResultSet rs = st.executeQuery(sql)){
//			final ResultSetMetaData meta = rs.getMetaData();
//			final int columnCount = meta.getColumnCount();
			while(rs.next()){
				final Map<String, String> row = Maps.newHashMap();
				for(int i=1; i<=columnNames.length; i++){
					row.put(columnNames[i-1], rs.getString(i));//这里不能用rs.getString(【columnNames[i]】)
				}
				r.add(row);
			}
			return r;
		}catch(Exception e){
			throw new IllegalArgumentException("查询sqlite失败", e);
		}
	}
	
	
	public static List<Map<String, Object>> getAllByColumnNames_2_2(final Connection conn, final String tableName, final String[] columnNames , final String where, final String orderBy, final Long offset, final Long limit){
		if(columnNames == null || columnNames.length <= 0){
			throw new IllegalArgumentException();
		}
		final StringBuilder sqlBuilder = new StringBuilder();
		sqlBuilder.append("select ").append(columnNamesStr(columnNames)).append(" from `").append(tableName).append("` ");
		if(!Strings.isNullOrEmpty(where)){
			sqlBuilder.append(" where ")/*.append(" where 1=1 ")*/.append(where).append(" ");
		}
		if(!Strings.isNullOrEmpty(orderBy)){
			sqlBuilder.append(" ").append(orderBy).append(" ");
		}
		if(limit != null && limit >= 0){
			sqlBuilder.append(" limit ").append(limit).append(" ");
		}
		if(offset != null && offset >= 0){
			sqlBuilder.append(" offset ").append(offset).append(" ");
		}
		final String sql = sqlBuilder.toString();
		final List<Map<String, Object>> r = Lists.newArrayList();
		try (final Statement st = conn.createStatement();final ResultSet rs = st.executeQuery(sql)){
//			final ResultSetMetaData meta = rs.getMetaData();
//			final int columnCount = meta.getColumnCount();
			while(rs.next()){
				final Map<String, Object> row = Maps.newHashMap();
				for(int i=1; i<=columnNames.length; i++){
					row.put(columnNames[i-1], rs.getString(i));//这里不能用rs.getString(【columnNames[i]】)
				}
				r.add(row);
			}
			return r;
		}catch(Exception e){
			throw new IllegalArgumentException("查询sqlite失败", e);
		}
	}
	
	
	
	static boolean isNull(final int type){
		return java.sql.Types.NULL == type;
	}
	
	static boolean isNumber(final int type){
		switch(type){
			case java.sql.Types.TINYINT: return true;
			case java.sql.Types.SMALLINT: return true;
			case java.sql.Types.INTEGER: return true;
			case java.sql.Types.BIGINT: return true;
			case java.sql.Types.FLOAT: return true;
			case java.sql.Types.REAL: return true;
			case java.sql.Types.DOUBLE: return true;
			case java.sql.Types.NUMERIC: return true;
			case java.sql.Types.DECIMAL: return true;
			default: return false;
		}
	}
	
	
	
	
	
	
	
	public static boolean update(final Connection conn, final String tableName, final String where, final Map<String, Object> kvs){
		if(kvs == null || kvs.size() <= 0){
			throw new IllegalArgumentException("要插入数据不能为空");
		}
		final Set<Entry<String, Object>> set = kvs.entrySet();
		final String[] columnNames = new String[set.size()];
		final Object[] columnValues = new Object[set.size()];
		int i=0;
		for(final Entry<String, Object> entry: set){
			columnNames[i] = entry.getKey();
			columnValues[i] = entry.getValue();
			i++;
		}
		return update(conn, tableName, where, columnValues, columnNames);
	}
	
	
	public static boolean update(final Connection conn, final String tableName, final String where, final List<Object> columnValues, final List<String> columnNames ){
		return update(conn, tableName, where, 
				columnValues!=null? columnValues.toArray(new Object[0]): new Object[0], 
				columnNames!=null? columnNames.toArray(new String[0]): new String[0]
				);
	}
	
	/**
	 * 像这样的：【UPDATE Person SET Address = 'Zhongshan 23', City = 'Nanjing' WHERE LastName = 'Wilson'】
	 * @param conn
	 * @param tableName
	 * @param where
	 * @param columnValues
	 * @param columnNames
	 * @return
	 */
	public static boolean update(final Connection conn, final String tableName, final String where, final Object[] columnValues, final String[] columnNames ){
		if(columnValues == null || columnValues.length <= 0 || columnNames == null || columnNames.length <= 0 || columnNames.length > columnValues.length){
			throw new IllegalArgumentException();
		}
		final StringBuilder psqlBuilder = new StringBuilder();
		final String columnNamesStr = Joiner.on(",").join(new Iterator<String>(){
			int index = 0;
			final int columnValueLength = columnValues.length;
			final int columnNameLength = columnNames.length;
			public boolean hasNext() {
				return index < columnValueLength && index < columnNameLength;
			}
			public String next() {
				return "`"+ columnNames[index++] + "`"  + " = ? ";
			}
		});
		psqlBuilder.append("UPDATE `").append(tableName).append("` SET ").append(columnNamesStr);
		if(!Strings.isNullOrEmpty(where)){
			psqlBuilder.append(" WHERE ").append(where).append(" ");
		}
		final String psql = psqlBuilder.toString();
		try (final PreparedStatement ps = conn.prepareStatement(psql)){
			//以columnName为准
			//columnValues的右边 可以有多余
			//columnValues如果长度不够，自动补为null
			for(int i=0; i<columnNames.length;i++){
				Object columnValue = columnValues[i];
				if(columnValue instanceof Integer){
					ps.setInt(i+1, (Integer) columnValue);
				}else if(columnValue instanceof Double){
					ps.setDouble(i+1, (Double) columnValue);
				}else if(columnValue instanceof Float){
					ps.setFloat(i+1, (Float) columnValue);
				}else {
					columnValue = (columnValue != null)?columnValue.toString(): null; 
					ps.setString(i+1, (String) columnValue);
				}
			}
			return ps.executeUpdate()> 0;
		}catch(Exception e){
			throw new IllegalArgumentException(String.format("update信息到sqlite失败，tableName=[%s]，where=[%s], columnValues=[%s]，columnNames=[%s]", tableName, where, columnValues, columnNames), e);
		}
	}
	
	
	/**
	 * 
	 * 1. insert into xxxtable values(?, ?, ?, ...)
	 * 2. insert into xxxtable (xx, yy, zz) values(?, ?, ?, ...)
	 * 
	 * 数值类型只支持int、String
	 * @param st
	 * @param tableName
	 * @param columnValues
	 * @param columnNames
	 * @return
	 */
	public static boolean insert(final Connection conn, final String tableName,final List<Object> columnValues, final List<String> columnNames ){
		return insert(conn, tableName, 
				columnValues!=null? columnValues.toArray(new Object[0]): new Object[0], 
				columnNames!=null? columnNames.toArray(new String[0]): new String[0]
				);
	}
	
	
	
	/**
	 * 
	 * 1. insert into xxxtable values(?, ?, ?, ...)
	 * 2. insert into xxxtable (xx, yy, zz) values(?, ?, ?, ...)
	 * 
	 * 数值类型只支持int、String
	 * @param st
	 * @param tableName
	 * @param columnValues
	 * @param columnNames
	 * @return
	 */
	public static boolean insert(final Connection conn, final String tableName,final Object[] columnValues, final String[] columnNames ){
		if(columnValues == null || columnValues.length <= 0){
			throw new IllegalArgumentException();
		}
		final StringBuilder psqlBuilder = new StringBuilder();
		if(columnNames != null && columnNames.length > 0){
			final String columnNamesStr = Joiner.on(",").join(new Iterator<String>(){
				int index = 0;
				public boolean hasNext() {
					return index < columnNames.length;
				}
				public String next() {
					return "`"+ columnNames[index++] + "`";
				}
			});
			psqlBuilder.append("INSERT INTO `").append(tableName).append("` ("+columnNamesStr+") VALUES ");
		}else{
			psqlBuilder.append("INSERT INTO `").append(tableName).append("` VALUES  ");
		}
		psqlBuilder.append("(? ");
		for(int i=1; i<columnValues.length; i++){
			psqlBuilder.append(",? ");
		}
		psqlBuilder.append(")");
		final String psql = psqlBuilder.toString();
		try (final PreparedStatement ps = conn.prepareStatement(psql)){
			if(columnNames != null && columnNames.length > 0){
				//以columnName为准
				//columnValues的右边 可以有多余
				//columnValues如果长度不够，自动补为null
				for(int i=0; i<columnNames.length;i++){
					Object columnValue = (i<columnValues.length) ? columnValues[i]: null;
					if(columnValue instanceof Integer){
						ps.setInt(i+1, (Integer) columnValue);
					}if(columnValue instanceof Double){
						ps.setDouble(i+1, (Double) columnValue);
					}if(columnValue instanceof Float){
						ps.setFloat(i+1, (Float) columnValue);
					}else{
						columnValue = (columnValue != null)?columnValue.toString(): null; 
						ps.setString(i+1, (String) columnValue);
					}
				}
			}else{
				for(int i=0; i<columnValues.length;i++){
					Object columnValue = columnValues[i];
					if(columnValue instanceof Integer){
						ps.setInt(i+1, (Integer) columnValue);
					}if(columnValue instanceof Double){
						ps.setDouble(i+1, (Double) columnValue);
					}if(columnValue instanceof Float){
						ps.setFloat(i+1, (Float) columnValue);
					}else{
						columnValue = (columnValue != null)?columnValue.toString(): null; 
						ps.setString(i+1, (String) columnValue);
					}
				}
			}
			return ps.executeUpdate()> 0;
		}catch(Exception e){
			throw new IllegalArgumentException(String.format("插入信息到sqlite失败，tableName=[%s]，columnValues=[%s]，columnNames=[%s]", tableName, columnValues, columnNames), e);
		}
	}
	
	
	/**
	 * 
	 * 1. insert into xxxtable values(?, ?, ?, ...)
	 * 2. insert into xxxtable (xx, yy, zz) values(?, ?, ?, ...)
	 * 
	 * 数值类型只支持int、String
	 */
	public static boolean insert(final Connection conn, final String tableName,final Map<String, Object> kvs){
		if(kvs == null || kvs.size() <= 0){
			throw new IllegalArgumentException("要插入数据不能为空");
		}
		final Set<Entry<String, Object>> set = kvs.entrySet();
		final String[] columnNames = new String[set.size()];
		final Object[] columnValues = new Object[set.size()];
		int i=0;
		for(final Entry<String, Object> entry: set){
			columnNames[i] = entry.getKey();
			columnValues[i] = entry.getValue();
			i++;
		}
		return insert(conn, tableName, columnValues, columnNames);
	}
	
	
	/**
	 * 支持linux、windows的路径格式。
	 * @param dbPath ---只能用绝对地址。
	 * @return
	 */
	public static Connection conn(final String dbPath){
		if(Strings.isNullOrEmpty(dbPath)){
			throw new IllegalArgumentException("dbPath的值为空");
		}
	    try {
	      return  DriverManager.getConnection("jdbc:sqlite:"+ dbPath);
	    } catch ( Exception e ) {
	    	throw new IllegalStateException("cannot open sqlite connection for daili_db_file_path", e);
	    }
	}
}
