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
	// ������̬����
	private static Configuration conf = null;

	static {
		conf = HBaseConfiguration.create();
		// ��classpath����ȡ��������Ϣ
		/*conf.set("hbase.zookeeper.quorum",
				"master");
		conf.set("hbase.zookeeper.property.clientPort", "2181");*/
	}

	// �������ݿ��
	public static void createTable(String tableName, String[] columnFamilys)
			throws Exception {
		// �½�һ�����ݿ����Ա
		HBaseAdmin hAdmin = new HBaseAdmin(conf);

		if (hAdmin.tableExists(tableName)) {
			System.out.println("���Ѿ�����");
			System.exit(0);
		} else {
			// �½�һ�� scores �������
			HTableDescriptor tableDesc = new HTableDescriptor(tableName);
			// ���������������
			for (String columnFamily : columnFamilys) {
				tableDesc.addFamily(new HColumnDescriptor(columnFamily));
			}
			// �������úõ���������
			hAdmin.createTable(tableDesc);
			System.out.println("������ɹ�");
		}
	}

	// ɾ�����ݿ��
	public static void deleteTable(String tableName) throws Exception {
		// �½�һ�����ݿ����Ա
		HBaseAdmin hAdmin = new HBaseAdmin(conf);

		if (hAdmin.tableExists(tableName)) {
			// �ر�һ����
			hAdmin.disableTable(tableName);
			// ɾ��һ����
			hAdmin.deleteTable(tableName);
			System.out.println("ɾ����ɹ�");

		} else {
			System.out.println("ɾ���ı�����");
			System.exit(0);
		}
	}

	// ���һ������
	public static void addRow(HTable table, String row,
			String columnFamily, String column, String value) throws Exception {
		Put put = new Put(Bytes.toBytes(row));
		// �������ֱ����塢�С�ֵ
		put.add(Bytes.toBytes(columnFamily), Bytes.toBytes(column),
				Bytes.toBytes(value));
		table.put(put);
	}

	// ɾ��һ������
	public static void delRow(HTable table, String row) throws Exception {
		Delete del = new Delete(Bytes.toBytes(row));
		table.delete(del);
	}

	// ɾ����������
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
		// ������
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
		// ������
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

			// ��һ�����������ݿ����users2��
			String[] columnFamilys = { "info", "course" };
			HBaseApi.createTable(tableName, columnFamilys);

			// �ڶ����������ݱ���������
			// ��ӵ�һ������
			HBaseApi.addRow(table, "tht", "info", "age", "20");
			HBaseApi.addRow(table, "tht", "info", "sex", "boy");
			HBaseApi.addRow(table, "tht", "course", "china", "97");
			HBaseApi.addRow(table, "tht", "course", "math", "128");
			HBaseApi.addRow(table, "tht", "course", "english", "85");
			// ��ӵڶ�������
			HBaseApi.addRow(table, "xiaoxue", "info", "age", "19");
			HBaseApi.addRow(table, "xiaoxue", "info", "sex", "boy");
			HBaseApi.addRow(table, "xiaoxue", "course", "china", "90");
			HBaseApi.addRow(table, "xiaoxue", "course", "math", "120");
			HBaseApi.addRow(table, "xiaoxue", "course", "english", "90");
			// ��ӵ���������
			HBaseApi.addRow(table, "qingqing", "info", "age", "18");
			HBaseApi.addRow(table, "qingqing", "info", "sex", "girl");
			HBaseApi
					.addRow(table, "qingqing", "course", "china", "100");
			HBaseApi.addRow(table, "qingqing", "course", "math", "100");
			HBaseApi.addRow(table, "qingqing", "course", "english",
					"99");
			// ����������ȡһ������
			System.out.println("��ȡһ������");
			HBaseApi.getRow(table, "tht");
			// ���Ĳ�����ȡ��������
			System.out.println("��ȡ��������");
			HBaseApi.getAllRows(table);
			// ���岽��ɾ��һ������
			System.out.println("ɾ��һ������");
			HBaseApi.delRow(table, "tht");
			HBaseApi.getAllRows(table);
			// ��������ɾ����������
			System.out.println("ɾ����������");
			String[] rows = { "xiaoxue", "qingqing" };
			HBaseApi.delMultiRows(table, rows);
			HBaseApi.getAllRows(table);
			// �ڰ˲���ɾ�����ݿ�
			System.out.println("ɾ�����ݿ�");
			HBaseApi.deleteTable(tableName);

		} catch (Exception err) {
			err.printStackTrace();
		}
	}
}