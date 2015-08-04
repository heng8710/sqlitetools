 package tools;


import java.sql.Connection;
import java.sql.DriverManager;

import com.google.common.base.Strings;

public class SqliteTools {
	 
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
	
	
	
//	public static Optional<DailiDomain> getLastOne(){
//		final String sql = "select * from dali order by addTime desc limit 0,1";
//		try (final Connection conn = conn();final Statement st = conn.createStatement();final ResultSet rs = st.executeQuery(sql)){
//			if(rs.next()){
//				final Long id = rs.getLong("id");
//				final String title = rs.getString("title");
//				final String columnNames = rs.getString("columnNames");
//				final String rows = rs.getString("rows");
//				final Long addTime = rs.getLong("addTime");
//				final String reserveText = rs.getString("reserveText");
//				final String comment = rs.getString("comment");
//				
//				final DailiDomain r =new DailiDomain();
//				r.setId(id);
//				r.setTitle(title);
//				r.setColumnNames(columnNames);
//				r.setRows(rows);
//				r.setAddTime(addTime);
//				r.setReserveText(reserveText);
//				r.setComment(comment);
//				return Optional.of(r);
//			}else{
//				return Optional.absent();
//			}
//		}catch(Exception e){
//			throw new IllegalArgumentException("查询最后一条代理信息到sqlite失败", e);
//		}
//	}
//	
//	
//	
//	public static List<DailiDomain> getAll(){
//		final String sql = "select * from dali order by addTime desc limit";
//		final List<DailiDomain> list = Lists.newArrayList();
//		try (final Connection conn = conn();final Statement st = conn.createStatement();final ResultSet rs = st.executeQuery(sql)){
//			while(rs.next()){
//				final Long id = rs.getLong("id");
//				final String title = rs.getString("title");
//				final String columnNames = rs.getString("columnNames");
//				final String rows = rs.getString("rows");
//				final Long addTime = rs.getLong("addTime");
//				final String reserveText = rs.getString("reserveText");
//				final String comment = rs.getString("comment");
//				
//				final DailiDomain r =new DailiDomain();
//				r.setId(id);
//				r.setTitle(title);
//				r.setColumnNames(columnNames);
//				r.setRows(rows);
//				r.setAddTime(addTime);
//				r.setReserveText(reserveText);
//				r.setComment(comment);
//				list.add(r);
//			}
//			return list;
//		}catch(Exception e){
//			throw new IllegalArgumentException("查询最后一条代理信息到sqlite失败", e);
//		}
//	}
//	
//	
//	public static boolean insert(final PageDomain pageDomain){
//		final String psql = "insert into daili (title, columnNames, rows, addTime, reserveText, comment) values "
//				+ "(?, ?, ?, ?, ?, ?)";
//		try (final Connection conn = conn();final PreparedStatement ps = conn.prepareStatement(psql)){
//			ps.setString(1, pageDomain.getTitle());
//			ps.setString(2, Joiner.on(',').join(pageDomain.getColumnNames()));
//			ps.setString(3, new ObjectMapper().writeValueAsString(pageDomain.getRows()));
//			//到秒就可以了
//			ps.setLong(4, pageDomain.getTime().getTime()/1000L);
//			ps.setString(5, "");
//			ps.setString(6, "");
//			return ps.execute();
//		}catch(Exception e){
//			throw new IllegalArgumentException(String.format("抛入代理信息到sqlite失败，抛入的信息是=[%s]", pageDomain), e);
//		}
//	}
	
	
	
	
	
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
