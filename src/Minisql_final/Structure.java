package Minisql_final;

import java.util.ArrayList;
import java.util.Vector;

public class Structure {
	
	public static class Attribute {	
		public boolean isPrimeryKey;
		public int length;
		public String name;
		public int offset;
		public int type;//1->int ,2->float, 3->char
		public boolean unique;	
	}

	public enum Comparison {
		Eq, Ge, Gt, Le, Ls, Ne
	}

	public static  class Condition {
		public int columnNum;  
		public Comparison op; 
		public String value;   
	}
	
	public static class Data {
		public Vector<row> Lines;
		Data()
		{
			Lines= new Vector<row>();
		}
	}

	public static class Index {
		
		public int blockNum=1;		//number of block the datas of the index occupied in the file index_name.table
		public int column;			//on which column the index is created
		public int columnLength;
		public String indexName;	//all the datas is store in file index_name.index
		public int rootNum;
		public String tableName;	//the name of the table on which the index is create
		

	}
	
	public static class offsetInfo {
		public int offsetInBlock;
		public int offsetInfile;
	}
	
	public static class Record {
		public Vector<byte[]> columns;
			
			Record()
			{
				columns = new Vector<byte[]>();
			}
			
			Record selectRecord(Table tableInfo,selectAttribute selections){
				Record returnRecord=new Record();
				
				for(int i=0;i<tableInfo.attriNum;i++)
					for(int j=0;j<selections.columns.size();j++)
						if(i==selections.columns.get(j)){
							returnRecord.columns.addElement(this.columns.get(i));
							break;
						}
				
				return returnRecord; 
			}
		}

	public static class row {
		public Vector<String> columns;
		row()
		{
			columns = new Vector<String>();
		}
	}
	
	public static class selectAttribute {
		Vector<Integer> columns;
	}

	public static class Table {
		
		public int attriNum;	//the number of attributes in the tables
		public ArrayList<Attribute> attrlist;
		public int blockNum;	//number of block the datas of the table occupied in the file name.table
		
		public int maxRecordsPerBlock;
		public String primaryKey;
		public int recordLength;	//total length of one record, should be equal to sum(attributes[i].length)
		public String tableName;
		
		
		Table()
		{
			tableName = null;
			primaryKey = null;
			blockNum = 1;
			recordLength = 0;
			maxRecordsPerBlock = 0;
		}
		
	}

}
