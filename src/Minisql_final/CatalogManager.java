package Minisql_final;

import Minisql_final.Structure.*;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.Vector;

public class CatalogManager {

	public final static int CHAR = 3;

	public final static int FLOAT = 2;

	public final static int INT = 1;
	
	public static String myfile = "";
	
	protected static LinkedList<Index> indexList= new LinkedList<Index>();

	protected static LinkedList<Table> tableList= new LinkedList<Table>();
	
	
	public static void ReadCatalog() {
		Table table;
		Attribute attribute;
		Index index;
		int length = 0;
		File file = new File(myfile+"tables.catalog");
		
		if (!file.exists()) {
			try {
				file.createNewFile();
				BufferedWriter writer = new BufferedWriter(new FileWriter(file));
				String str = "-1\n-1\n";
				writer.write(str);
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		BufferedReader br;
		try {
			br = new BufferedReader(new FileReader(myfile+"tables.catalog"));
			
			String data = br.readLine();
			while (!data.equals("-1")) {
				table = new Table();
				table.tableName = data;
				data = br.readLine();
				table.primaryKey = data;
				data = br.readLine();
				table.attriNum = Integer.parseInt(data);
				data = br.readLine();
				table.blockNum = Integer.parseInt(data);
				data = br.readLine();
				table.recordLength = Integer.parseInt(data);
				data = br.readLine();
				table.maxRecordsPerBlock = Integer.parseInt(data);
				table.attrlist=new ArrayList<Attribute>();
				data = br.readLine();
				while (!data.equals("-1")) {
					attribute = new Attribute();
					attribute.name = data;
					attribute.offset = length;
					if (attribute.name.equals(table.primaryKey))
						attribute.isPrimeryKey = true;
					else
						attribute.isPrimeryKey = false;
					data = br.readLine();
					if (data.equals("INT")) {
						attribute.type = INT;
						attribute.length = 4;
						length += attribute.length;
					} else if (data.equals("FLOAT")) {
						attribute.type = FLOAT;
						attribute.length = 4;
						length += attribute.length;
					} else {
						attribute.type = CHAR;
						data = br.readLine();
						attribute.length = Integer.parseInt(data);
						length += attribute.length;
					}
					data = br.readLine();
					attribute.unique= data.toString().equals("true")?true:false;
					table.attrlist.add(attribute);
					data = br.readLine();
				}
				length = 0;
				tableList.add(table);
				data = br.readLine();
			}
			br.close();

		} catch (IOException e) {
			e.printStackTrace();
		}

		file = new File(myfile+"index.catalog");
		
		if (!file.exists()) {
			try {
				file.createNewFile();
				BufferedWriter writer = new BufferedWriter(new FileWriter(file));
				String str = "-1\n";	
				writer.write(str);
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		try {
			
			br = new BufferedReader(new FileReader(myfile+"index.catalog"));
			String data = br.readLine();
			while (!data.equals("-1")) {
				index = new Index();
				
				index.indexName = data;
				index.tableName = br.readLine();
				index.column= Integer.parseInt(br.readLine());
				index.columnLength= Integer.parseInt(br.readLine());
				index.rootNum= Integer.parseInt(br.readLine());
				index.blockNum= Integer.parseInt(br.readLine());			
				data = br.readLine();
				indexList.add(index);
				
				
			}
			br.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}

	public static void UpdateCatalog() {
		int length = 0;
		String str;
		File file;
		if (!tableList.isEmpty()) {
			file = new File(myfile+"tables.catalog");
			if (!file.exists())
				try {
					file.createNewFile();
				} catch (IOException e) {
					e.printStackTrace();
				}
			try {
				BufferedWriter writer = new BufferedWriter(new FileWriter(file));
				for (Table table : tableList) {
					str = table.tableName + "\n" + table.primaryKey + "\n";
					str += table.attriNum + "\n" + table.blockNum + "\n" + table.recordLength + "\n" + table.maxRecordsPerBlock + "\n";
					if (!table.attrlist.isEmpty()) {
						for (Attribute attribute : table.attrlist) {
							str += attribute.name + "\n";
							if (attribute.type == INT) {
								str += "INT\n";
							} else if (attribute.type == FLOAT) {
								str += "FLOAT\n";
							} else {
								length = attribute.length;
								str += "CHAR\n" + length + "\n";
							}
							str += attribute.unique + "\n";
						}
						str += "-1\n";
						writer.write(str);
					}
				}
				writer.write("-1\n");
				writer.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
		file = new File(myfile+"index.catalog");	
		if (!file.exists())
			try {
				file.createNewFile();
			} catch (IOException e) {
				e.printStackTrace();
			}
		
		try {
			BufferedWriter writer = new BufferedWriter(new FileWriter(file));
			if (!indexList.isEmpty()) {
				for (Index index : indexList) {
					str = index.indexName + "\n" + index.tableName + "\n" + Integer.toString(index.column)+"\n"+Integer.toString(index.columnLength)+"\n"+Integer.toString(index.rootNum)+"\n"+Integer.toString(index.blockNum) + "\n";					
					writer.write(str);
				}
			}
			writer.write("-1\n");
			writer.close();
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	
	public static void Create_Index(Index index) {

		indexList.add(index);
		// TODO Auto-generated method stub
	}

	public static void Create_Table(Table table) {
		
		tableList.add(table);
		// TODO Auto-generated method stub
	}

	public static void Drop_Index(String indexName) {
	
		
		Index index = getIndex(indexName);
		if ( index != null) {
			indexList.remove(index);
		}
		else {
			System.out.println("Index info not found");
		}
	}

	public static void Drop_Table(String tableName) {
		Table table = getTable(tableName);
		if (table!= null) {
			tableList.remove(table);
		}
		else {
			System.out.println("Table info not found");
		}
		// TODO Auto-generated method stub
	}

	public static Index getIndex(String indexName) {
		
		
		for (Index index : indexList) {
			/*
				for (Table table : tableList) {
					if(table.attrlist.get(index.column).name.equals(indexName))
						return index;
				}
*/
				if (index.indexName.equals(indexName)) {
					return index;
				}
				
		}
		// TODO Auto-generated method stub
		return null;
	}

	public static Index getIndexfromTable(String tableName, String attrName) {
		
		
		Table temptable = null;
		for (Table table : tableList) {
			if (table.tableName.equals(tableName)) {
				temptable = table;
				break;
			}
		}
		
		if (temptable!= null) {
			for (Index index : indexList) {
				if (index.tableName.equals(tableName)) {
					if (temptable.attrlist.get(index.column).name.equals(attrName) ) {
						return index;
					}
				}
			}
		}
		// TODO Auto-generated method stub
		return null;
	}

	public static Table getTable(String tableName) {
		
		for (Table table : tableList) {
			if (table.tableName.equals(tableName)) {
				return table;
			}
		}
		// TODO Auto-generated method stub
		return null;
	}

	public static boolean isAttribution(String tablename, String attributionname) {
		
		for (Table table : tableList) {
			if (table.tableName.equals(tablename)) {
				for (Attribute attribute : table.attrlist) {
					if (attribute.name.equals(attributionname)) {
						return true;
					}
				}
			}
		}
		// TODO Auto-generated method stub
		return false;
	}

	public static boolean matchType(String word, String tablename, int count) {
		int type = 0;
		for (Table table: tableList) {
			if (table.tableName.equals(tablename)) {
				type = table.attrlist.get(count-1).type;
			}
		}
		
		switch(type){
		  case 1:if(word.matches("[0-9]*")) return true;break;//int
		  case 2:if(word.matches("[0-9]*|([0-9]*.[0-9]*)")) return true;break;//float
		  case 3: if (word.matches("'[a-zA-Z0-9_]*'")&&(type==3)) return true;break;//char
	  }
		
		// TODO Auto-generated method stub
		return false;
	}

	public static Vector<String> relativaIndex(String tableName) {
		
		Vector<String> allIndex = new Vector<String>();
		for (Index index : indexList) {
			if (index.tableName.equals(tableName)) {
				allIndex.add(index.indexName);
			}
		}
		// TODO Auto-generated method stub
		return allIndex;
	}

	public static void setIndexRoot(String indexName, int num) {
		for (Index index : indexList) {
			if (index.indexName.equals(indexName)) {
				index.rootNum = num;
			}
		}
		// TODO Auto-generated method stub
	}

	public static boolean Type(String att, String word, String tablename) {
		int type = 0;
		for (Table table: tableList) {
			if (table.tableName.equals(tablename)) {
				for (Attribute attribute : table.attrlist) {
					if (att.equals(attribute.name)) {
						type = attribute.type;
					}
				}
			}
		}
		switch(type){
	  	
		  case 1:if(word.matches("[0-9]*")) return true;break;//int
		  case 2:if(word.matches("[0-9]*|([0-9]*.[0-9]*)")) return true;break;//float
		  case 3: if (word.matches("'[a-zA-Z0-9_]*'")&&(type==3)) return true;break;//char
	  }
		// TODO Auto-generated method stub
		return false;
	}

}
