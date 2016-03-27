package com.njp.learn.hadoop_learn;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HBaseAdmin;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.util.Bytes;

public class HBaseApi {
	// 声明静态配置
	private static Configuration conf = null;

	static {
		conf = HBaseConfiguration.create();
		// 从classpath里面取到配置信息
		/*conf.set("hbase.zookeeper.quorum",
				"master");
		conf.set("hbase.zookeeper.property.clientPort", "2181");*/
	}

	// 创建数据库表
	public static void createTable(String tableName, String[] columnFamilys)
			throws Exception {
		// 新建一个数据库管理员
		HBaseAdmin hAdmin = new HBaseAdmin(conf);

		if (hAdmin.tableExists(tableName)) {
			System.out.println("表已经存在");
			System.exit(0);
		} else {
			// 新建一个 scores 表的描述
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			// 在描述里添加列族
			for (String columnFamily : columnFamilys) {
				tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			}
			// 根据配置好的描述建表
			hAdmin.createTable(tableDesc);
			System.out.println("创建表成功");
		}
	}

	// 删除数据库表
	public static void deleteTable(String tableName) throws Exception {
		// 新建一个数据库管理员
		HBaseAdmin hAdmin = new HBaseAdmin(conf);

		if (hAdmin.tableExists(tableName)) {
			// 关闭一个表
			hAdmin.disableTable(tableName);
			// 删除一个表
			hAdmin.deleteTable(tableName);
			System.out.println("删除表成功");

		} else {
			System.out.println("删除的表不存在");
			System.exit(0);
		}
	}

	// 添加一条数据
	public static void addRow(HTable table, String row,
			String columnFamily, String column, String value) throws Exception {
		Put put = new Put(Bytes.toBytes(row));
		// 参数出分别：列族、列、值
		put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
				Bytes.toBytes(value));
		table.put(put);
	}

	// 删除一条数据
	public static void delRow(HTable table, String row) throws Exception {
		Delete del = new Delete(Bytes.toBytes(row));
		table.delete(del);
	}

	// 删除多条数据
	public static void delMultiRows(HTable table, String[] rows)
			throws Exception {

		List<Delete> list = new ArrayList<Delete>();

		for (String row : rows) {
			Delete del = new Delete(Bytes.toBytes(row));
			list.add(del);
		}

		table.delete(list);
	}

	// get row
	public static void getRow(HTable table, String row) throws Exception {
		Get get = new Get(Bytes.toBytes(row));
		Result result = table.get(get);
		// 输出结果
		for (KeyValue rowKV : result.raw()) {
			System.out.print("Row Name: " + new String(rowKV.getRow()) + " ");
			System.out.print("Timestamp: " + rowKV.getTimestamp() + " ");
			System.out.print("column Family: " + new String(rowKV.getFamily()) + " ");
			System.out.print("Row Name:  " + new String(rowKV.getQualifier()) + " ");
			System.out.println("Value: " + new String(rowKV.getValue()) + " ");
		}
	}

	// get all records
	public static void getAllRows(HTable table) throws Exception {
		
		Scan scan = new Scan();
		ResultScanner results = table.getScanner(scan);
		// 输出结果
		for (Result result : results) {
			for (KeyValue rowKV : result.raw()) {
				System.out.print("Row Name: " + new String(rowKV.getRow()) + " ");
				System.out.print("Timestamp: " + rowKV.getTimestamp() + " ");
				System.out.print("column Family: " + new String(rowKV.getFamily()) + " ");
				System.out
						.print("Row Name:  " + new String(rowKV.getQualifier()) + " ");
				System.out.println("Value: " + new String(rowKV.getValue()) + " ");
			}
		}
	}

	// main
	public static void main(String[] args) {
		try {
			String tableName = "users2";
			HTable table = new HTable(conf, tableName);

			// 第一步：创建数据库表：“users2”
			String[] columnFamilys = { "info", "course" };
			HBaseApi.createTable(tableName, columnFamilys);

			// 第二步：向数据表的添加数据
			// 添加第一行数据
			HBaseApi.addRow(table, "tht", "info", "age", "20");
			HBaseApi.addRow(table, "tht", "info", "sex", "boy");
			HBaseApi.addRow(table, "tht", "course", "china", "97");
			HBaseApi.addRow(table, "tht", "course", "math", "128");
			HBaseApi.addRow(table, "tht", "course", "english", "85");
			// 添加第二行数据
			HBaseApi.addRow(table, "xiaoxue", "info", "age", "19");
			HBaseApi.addRow(table, "xiaoxue", "info", "sex", "boy");
			HBaseApi.addRow(table, "xiaoxue", "course", "china", "90");
			HBaseApi.addRow(table, "xiaoxue", "course", "math", "120");
			HBaseApi.addRow(table, "xiaoxue", "course", "english", "90");
			// 添加第三行数据
			HBaseApi.addRow(table, "qingqing", "info", "age", "18");
			HBaseApi.addRow(table, "qingqing", "info", "sex", "girl");
			HBaseApi
					.addRow(table, "qingqing", "course", "china", "100");
			HBaseApi.addRow(table, "qingqing", "course", "math", "100");
			HBaseApi.addRow(table, "qingqing", "course", "english",
					"99");
			// 第三步：获取一条数据
			System.out.println("获取一条数据");
			HBaseApi.getRow(table, "tht");
			// 第四步：获取所有数据
			System.out.println("获取所有数据");
			HBaseApi.getAllRows(table);
			// 第五步：删除一条数据
			System.out.println("删除一条数据");
			HBaseApi.delRow(table, "tht");
			HBaseApi.getAllRows(table);
			// 第六步：删除多条数据
			System.out.println("删除多条数据");
			String[] rows = { "xiaoxue", "qingqing" };
			HBaseApi.delMultiRows(table, rows);
			HBaseApi.getAllRows(table);
			// 第八步：删除数据库
			System.out.println("删除数据库");
			HBaseApi.deleteTable(tableName);

		} catch (Exception err) {
			err.printStackTrace();
		}
	}
}