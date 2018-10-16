package net.newstring.lyra.test.simple;

import lombok.Data;
import org.influxdb.InfluxDB;
import org.influxdb.InfluxDBFactory;
import org.influxdb.dto.Point;
import org.influxdb.dto.Query;
import org.influxdb.dto.QueryResult;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.springframework.beans.BeanWrapperImpl;

import java.io.Serializable;
import java.util.*;

public class BasicOperationTestTest {

    private InfluxDBConnect influxDB;
    private String username = "admin";//用户名
    private String password = "admin";//密码
    private String openurl = "http://127.0.0.1:8086";//连接地址
    private String database = "test_db";//数据库
    private String measurement = "sys_code";//相当于数据表

    @Before
    public void setUp() {
        //创建 连接
        influxDB = new InfluxDBConnect(username, password, openurl, database);
        influxDB.influxDbBuild();
        influxDB.createRetentionPolicy();
//		influxDB.deleteDB(database);
//		influxDB.createDB(database);
    }

    @Test
    public void testInsert() {//测试数据插入
        Map<String, String> tags = new HashMap<String, String>();
        Map<String, Object> fields = new HashMap<String, Object>();
        List<CodeInfo> list = new ArrayList<CodeInfo>();

        CodeInfo info1 = new CodeInfo();
        info1.setId(1L);
        info1.setName("BANKS");
        info1.setCode("ABC");
        info1.setDescr("中国农业银行");
        info1.setDescrE("ABC");
        info1.setCreatedBy("system");
        info1.setCreatedAt(new Date().getTime());

        CodeInfo info2 = new CodeInfo();
        info2.setId(2L);
        info2.setName("BANKS");
        info2.setCode("CCB");
        info2.setDescr("中国建设银行");
        info2.setDescrE("CCB");
        info2.setCreatedBy("system");
        info2.setCreatedAt(new Date().getTime());
        list.add(info1);
        list.add(info2);

        for (CodeInfo info : list) {

            tags.put("TAG_CODE", info.getCode());
            tags.put("TAG_NAME", info.getName());

            fields.put("ID", info.getId());
            fields.put("NAME", info.getName());
            fields.put("CODE", info.getCode());
            fields.put("DESCR", info.getDescr());
            fields.put("DESCR_E", info.getDescrE());
            fields.put("CREATED_BY", info.getCreatedBy());
            fields.put("CREATED_AT", info.getCreatedAt());

            influxDB.insert(measurement, tags, fields);
        }
    }

    @Test
    public void testQuery() {//测试数据查询
        String command = "select * from sys_code";
        QueryResult results = influxDB.query(command);
        if (results.getResults() == null) {
            return;
        }
        List<CodeInfo> lists = new ArrayList<CodeInfo>();
        for (QueryResult.Result result : results.getResults()) {
            List<QueryResult.Series> series = result.getSeries();
            for (QueryResult.Series serie : series) {
                List<List<Object>> values = serie.getValues();
                List<String> columns = serie.getColumns();
                lists.addAll(getQueryData(columns, values));
            }
        }
        Assert.assertTrue((!lists.isEmpty()));
        Assert.assertEquals(2, lists.size());
    }

    private List<CodeInfo> getQueryData(List<String> columns, List<List<Object>> values) {
        List<CodeInfo> lists = new ArrayList<CodeInfo>();
        for (List<Object> list : values) {
            CodeInfo info = new CodeInfo();
            BeanWrapperImpl bean = new BeanWrapperImpl(info);
            for (int i = 0; i < list.size(); i++) {
                String propertyName = setColumns(columns.get(i));//字段名
                Object value = list.get(i);//相应字段值
                bean.setPropertyValue(propertyName, value);
            }

            lists.add(info);
        }

        return lists;
    }


    /***转义字段***/
    private String setColumns(String column) {
        String[] cols = column.split("_");
        StringBuffer sb = new StringBuffer();
        for (int i = 0; i < cols.length; i++) {
            String col = cols[i].toLowerCase();
            if (i != 0) {
                String start = col.substring(0, 1).toUpperCase();
                String end = col.substring(1).toLowerCase();
                col = start + end;
            }
            sb.append(col);
        }
        return sb.toString();
    }


    @Data
    public class CodeInfo implements Serializable {
        private static final long serialVersionUID = 1L;
        private Long id;
        private String name;
        private String code;
        private String descr;
        private String descrE;
        private String createdBy;
        private Long createdAt;
        private String time;
        private String tagCode;
        private String tagName;
    }


    /**
     * 时序数据库 InfluxDB 连接
     *
     * @author Dai_LW
     */
    @Data
    public class InfluxDBConnect {
        private String username;//用户名
        private String password;//密码
        private String openurl;//连接地址
        private String database;//数据库
        private InfluxDB influxDB;

        public InfluxDBConnect(String username, String password, String openurl, String database) {
            this.username = username;
            this.password = password;
            this.openurl = openurl;
            this.database = database;
        }

        /**
         * 连接时序数据库；获得InfluxDB
         **/
        public InfluxDB influxDbBuild() {
            if (influxDB == null) {
                influxDB = InfluxDBFactory.connect(openurl, username, password);
                influxDB.databaseExists(database);
                influxDB.createDatabase(database);

            }
            return influxDB;
        }

        /**
         * 设置数据保存策略
         * defalut 策略名 /database 数据库名/ 30d 数据保存时限30天/ 1  副本个数为1/ 结尾DEFAULT 表示 设为默认的策略
         */
        public void createRetentionPolicy() {
            String command = String.format("CREATE RETENTION POLICY \"%s\" ON \"%s\" DURATION %s REPLICATION %s DEFAULT",
                    "defalut", database, "30d", 1);
            this.query(command);
        }

        /**
         * 查询
         *
         * @param command 查询语句
         * @return
         */
        public QueryResult query(String command) {
            return influxDB.query(new Query(command, database));
        }

        /**
         * 插入
         *
         * @param measurement 表
         * @param tags        标签
         * @param fields      字段
         */
        public void insert(String measurement, Map<String, String> tags, Map<String, Object> fields) {
            Point.Builder builder = Point.measurement(measurement);
            builder.tag(tags);
            builder.fields(fields);

            influxDB.write(database, "", builder.build());
        }

        /**
         * 删除
         *
         * @param command 删除语句
         * @return 返回错误信息
         */
        public String deleteMeasurementData(String command) {
            QueryResult result = influxDB.query(new Query(command, database));
            return result.getError();
        }

        /**
         * 创建数据库
         *
         * @param dbName
         */
        public void createDB(String dbName) {
            influxDB.createDatabase(dbName);
        }

        /**
         * 删除数据库
         *
         * @param dbName
         */
        public void deleteDB(String dbName) {
            influxDB.deleteDatabase(dbName);
        }


    }


}