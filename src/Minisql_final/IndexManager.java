package Minisql_final;

import Minisql_final.Structure.*;

import java.io.*;

public class IndexManager{
	 
	public static BufferManager  buf;
	
	IndexManager(BufferManager buffer){
		buf=buffer;
	}
		
	public static void createIndex(Table tableInfo,Index indexInfo){ 
      	       	
        	BPlusTree thisTree=new BPlusTree(indexInfo/*,buf*/); 
        	
        	String filename=tableInfo.tableName+".table";       	
        	try{   	
        		for(int blockOffset=0; blockOffset< tableInfo.blockNum; blockOffset++){
        			BufferBlock block = BufferManager.readBlock(filename,blockOffset);
        			for(int offset =0; offset < block.recordNum /*tableInfo.maxPerRecordNum*/; offset++){
        				int position = offset*tableInfo.recordLength; 
        				byte[] Record = block.getBytes(position, tableInfo.recordLength); 
        				byte[] key=getColumnValue(tableInfo,indexInfo,Record); 
        				thisTree.insert(key, blockOffset, offset); 
        			}
        		}
        	}catch(NullPointerException e){
        		System.err.println("must not be null for key.");
        	}
        	catch(Exception e){
        		System.err.println("the index has not been created.");
        	}
        	
        	CatalogManager.setIndexRoot(indexInfo.indexName, thisTree.myRootBlock.blockOffset);
        	System.out.println("create index success");
	}
		
	static public void deleteKey(Index indexInfo,byte[] deleteKey) throws Exception{
		try{
			//Index inx=CatalogManager.getIndex(indexInfo.indexName);
			BPlusTree thisTree=new BPlusTree(indexInfo,buf,indexInfo.rootNum);
			thisTree.delete(deleteKey);	
			CatalogManager.setIndexRoot(indexInfo.indexName, thisTree.myRootBlock.blockOffset);
		}catch(NullPointerException e){
			System.err.println();
		}
		
	}
	
	public static void dropIndex(String filename ){
		filename+=".index";
		File file = new File(filename);
		
		try{
			if(file.exists())
				if(file.delete())   
					System.out.println("The index file has been deleted");
			else
				System.out.println("File "+filename+" cannot be found");
        }catch(Exception   e){
            System.out.println(e.getMessage());
            System.out.println("Delete index error");
        }
			
		System.out.println("Delete the index success");
	}
	
	private static byte[] getColumnValue(Table  tableinfor,Index  indexinfor, byte[] row){
		
		int s_pos = 0, f_pos = 0;	
		for(int i= 0; i <= indexinfor.column; i++){ 
			s_pos = f_pos;
			f_pos+=tableinfor.attrlist.get(i).length;
		}
		byte[] colValue=new byte[f_pos-s_pos];
		for(int j=0;j<f_pos-s_pos;j++){
			colValue[j]=row[s_pos+j];
		}
		return colValue;
	}
	
	static public void insertKey(Index indexInfo,byte[] key,int blockOffset,int offset) throws Exception{
		try{
			//Index inx=CatalogManager.getIndex(indexInfo.indexName);
			BPlusTree thisTree=new BPlusTree(indexInfo,buf,indexInfo.rootNum);
			thisTree.insert(key, blockOffset, offset);	
			CatalogManager.setIndexRoot(indexInfo.indexName, thisTree.myRootBlock.blockOffset);
		}catch(NullPointerException e){
			System.err.println();
		}
		
	}
	
	public static offsetInfo searchEqual(Index indexInfo, byte[] key) throws Exception{
		offsetInfo off=new offsetInfo();
		try{
			BPlusTree thisTree=new BPlusTree(indexInfo,buf,indexInfo.rootNum); 
			off=thisTree.searchKey(key);  
			return off;
		}catch(NullPointerException e){
			System.err.println();
			return null;
		}
	}
	
}