package sqlitetools.idmapper;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;

import sqlitetools.SqliteTools;

/**
 * 1、这是【uuid, id】的一个映射。
 * 2、为了保护 递增的id的规律性。
 * 3、提供的操作只有3个： init（不怕有副作用）：初始化db和表；gen：添加新的<uuid, id>项目；查询：根据id、uuid查询。
 * ------最重要是的： 没有update，因为认为比较轻，如果数据更新的话，全部重新gen过就好了。
 * 4、增量的话，调用gen(startId, endId)，可以操作。
 * 5、???考虑也给id建立索引???
 * 
 * 
 * -----------------------------------------------
 * 注意点：生成的过程（gen）很慢，几千个都要等几分钟
 */
public final class UUIDMapper {
	private final Logger logger = Logger.getLogger("global");
	
	private final String sqliteFilePath;
	
	static final String createTableSql = "CREATE TABLE IF NOT EXISTS 'uuidmapper' ("
										+"'uuid'  TEXT(32) NOT NULL,"
										+"'id'  INTEGER,"
										+"PRIMARY KEY ('uuid')"
										+")";

	public UUIDMapper(final String sqliteFilePath) {
		this.sqliteFilePath = sqliteFilePath;
		initDb();
	}
	
	
	private void initDb(){
		try(final Connection conn = SqliteTools.conn(sqliteFilePath); final Statement st = conn.createStatement()){
			st.execute(createTableSql);
		} catch (SQLException e) {
			logger.log(Level.SEVERE, "初始化uuidmapper失败", e);
		}
	}
	
	public void gen(final long id){
		final String psql = "insert into `uuidmapper` (`uuid`, `id`) values(?, ?)";
		final String uuid = nextId();
		try(final Connection conn = SqliteTools.conn(sqliteFilePath); final PreparedStatement ps = conn.prepareStatement(psql)){
			ps.setString(1, uuid);
			ps.setLong(2, id);
			ps.execute();
		} catch (SQLException e) {
//			logger.log(Level.SEVERE, String.format("插入新的uuid=[%s, %s]失败", uuid, id), e);
			throw new IllegalStateException(String.format("插入新的uuid=[%s, %s]失败", uuid, id), e);
		}
	}
	
	
	public void gen(final long startId, final long endId){
		for(long index=startId; index <= endId; index++){
			final String psql = "insert into `uuidmapper` (`uuid`, `id`) values(?, ?)";
			final String uuid = nextId();
			try(final Connection conn = SqliteTools.conn(sqliteFilePath); final PreparedStatement ps = conn.prepareStatement(psql)){
				ps.setString(1, uuid);
				ps.setLong(2, index);
				ps.execute();
			} catch (SQLException e) {
				logger.log(Level.SEVERE, String.format("插入新的uuid=[%s, %s]失败", uuid, index), e);
//				throw new IllegalStateException(String.format("插入新的uuid=[%s, %s]失败", uuid, id), e);
			}
		}
	}
	
	public Long id(final String uuid){
		final String sql = "select `id` from `uuidmapper` where `uuid` = '"+ uuid + "' limit 1 offset 0 ";
		try(final Connection conn = SqliteTools.conn(sqliteFilePath); final Statement st = conn.createStatement(); final ResultSet rs = st.executeQuery(sql)){
			if(rs.next()){
				return rs.getLong(1);
			}
			return null;
		} catch (SQLException e) {
//			logger.log(Level.SEVERE, String.format("插入新的uuid=[%s, %s]失败", uuid, id), e);
			throw new IllegalStateException(String.format("查询uuid=[%s]失败", uuid), e);
		}
	}
	
	public String uuid(final long id){
		final String sql = "select `uuid` from `uuidmapper` where `id` = "+ id + " limit 1 offset 0 ";
		try(final Connection conn = SqliteTools.conn(sqliteFilePath); final Statement st = conn.createStatement(); final ResultSet rs = st.executeQuery(sql)){
			if(rs.next()){
				return rs.getString(1);
			}
			return null;
		} catch (SQLException e) {
//			logger.log(Level.SEVERE, String.format("插入新的uuid=[%s, %s]失败", uuid, id), e);
			throw new IllegalStateException(String.format("查询id=[%s]失败", id), e);
		}
	}
	
	
	private String nextId(){
		return UUID.randomUUID().toString().replaceAll("-", "");//[a-z0-9]
	}
	
}
