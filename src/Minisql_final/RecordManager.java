package Minisql_final;

import Minisql_final.Structure.*;


import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.PrintWriter;
import java.io.UnsupportedEncodingException;
import java.util.Vector;



public class RecordManager {

	public static final int CHAR =3;
	public static final int FLOAT =2;
	public static final int INT =1;
		
	private static row bytesToString(Table tableInfo,Record recordLine) throws UnsupportedEncodingException{
			row returnRow=new row();		
			String tmpString = null;
			int col=0;
				for(int i=0;i<tableInfo.attriNum;i++){
				switch(tableInfo.attrlist.get(i).type){
				case CHAR:		
					tmpString=new String(recordLine.columns.get(i));
					break;
				case INT:
					int intvalue=0;
					for(int j=0;j<4;j++){
						intvalue  +=(recordLine.columns.get(i)[j] & 0xFF)<<(8*(3-j));
					}
					tmpString=new String(""+intvalue);
					break;
				case FLOAT:
					float flvalue=0;
					int l;
					l = recordLine.columns.get(i)[0]; 
					l &= 0xff; 
					l |= ((long) recordLine.columns.get(i)[ 1] << 8); 
					l &= 0xffff; 
					l |= ((long) recordLine.columns.get(i)[ 2] << 16); 
					l &= 0xffffff; 
					l |= ((long)recordLine.columns.get(i)[ 3] << 24); 
					flvalue= Float.intBitsToFloat(l);
					tmpString=new String(""+flvalue);
					break;
				}	
				returnRow.columns.add(tmpString);
			}
			
			return returnRow;
		}
		
		/*get the row(vector<String>) of the record according to the tableInfo*/
	private static row bytesToString(Table tableInfo,Record recordLine,selectAttribute selections) throws UnsupportedEncodingException{
			row returnRow=new row();		
			String tmpString = null;
			int col=0;
				//for(int i=0;i<tableInfo.attriNum;i++){
			for(int i=0;i<selections.columns.size();i++){
				col=selections.columns.get(i);
				switch(tableInfo.attrlist.get(col).type){
				case CHAR:		
					tmpString=new String(recordLine.columns.get(i));
					break;
				case INT:
					int intvalue=0;
					for(int j=0;j<4;j++){
						intvalue  +=(recordLine.columns.get(i)[j] & 0xFF)<<(8*(3-j));
					}
					tmpString=new String(""+intvalue);
					break;
				case FLOAT:
					float flvalue=0;
					int l;
					l = recordLine.columns.get(i)[0]; 
					l &= 0xff; 
					l |= ((long) recordLine.columns.get(i)[ 1] << 8); 
					l &= 0xffff; 
					l |= ((long) recordLine.columns.get(i)[ 2] << 16); 
					l &= 0xffffff; 
					l |= ((long)recordLine.columns.get(i)[ 3] << 24); 
					flvalue= Float.intBitsToFloat(l);
					tmpString=new String(""+flvalue);
					break;
				}	
				returnRow.columns.add(tmpString);
			}
			
			return returnRow;
		}
	
	private static boolean Compare(Table tableInfo,Record InfoLine,Vector<Condition> conditions) throws UnsupportedEncodingException{
		//note the conditions includes all the attributes,and may be in disorder
		for(int i=0;i<conditions.size();i++){ 
			int column=conditions.get(i).columnNum;  
			//String value1= new String(InfoLine.columns.get(column),"ISO-8859-1"); 
			String value2=conditions.get(i).value;
			
			switch(tableInfo.attrlist.get(column).type){ 
			case CHAR:
				String value1= new String(InfoLine.columns.get(column),"ISO-8859-1");
				switch(conditions.get(i).op){
				case Ls:
					if(value1.compareTo(value2)>=0) return false;	break;
				case Le:
					if(value1.compareTo(value2)>0) return false;	break;
				case Gt:
					if(value1.compareTo(value2)<=0) return false;	break;
				case Ge:
					if(value1.compareTo(value2)<0) return false;	break;
				case Eq:
					if(value1.compareTo(value2)!=0) return false;	break;
				case Ne:
					if(value1.compareTo(value2)==0) return false;	break;
				}
				break;
			case INT:
				int intvalue1=0;
				for(int j=0;j<4;j++){
					intvalue1  +=(InfoLine.columns.get(column)[j] & 0xFF)<<(8*(3-j));
				}
				int intvalue2=Integer.valueOf(value2).intValue();
				switch(conditions.get(i).op){
				case Ls:
					if(intvalue1>=intvalue2) return false;	break;
				case Le:
					if(intvalue1>intvalue2) return false;	break;
				case Gt:
					if(intvalue1<=intvalue2) return false;	break;
				case Ge:
					if(intvalue1<intvalue2) return false;	break;
				case Eq:
					if(intvalue1!=intvalue2) return false;	break;
				case Ne:
					if(intvalue1==intvalue2) return false;	break;
				}
				break;
			case FLOAT:
				float flvalue1=0;
				int l;
				l = InfoLine.columns.get(column)[0]; 
				l &= 0xff; 
				l |= ((long) InfoLine.columns.get(column)[ 1] << 8); 
				l &= 0xffff; 
				l |= ((long) InfoLine.columns.get(column)[ 2] << 16); 
				l &= 0xffffff; 
				l |= ((long) InfoLine.columns.get(column)[ 3] << 24); 
				flvalue1= Float.intBitsToFloat(l);
				float flvalue2=Float.valueOf(value2).floatValue();
				switch(conditions.get(i).op){
				case Ls:
					if(flvalue1>=flvalue2) return false;	break;
				case Le:
					if(flvalue1>flvalue2) return false;		break;
				case Gt:
					if(flvalue1<=flvalue2) return false;	break;
				case Ge:
					if(flvalue1<flvalue2) return false;		break;
				case Eq:
					if(flvalue1!=flvalue2) return false;	break;
				case Ne:
					if(flvalue1==flvalue2) return false;	break;
				}
				break;
			}
		}
		return true;
	}
	
	
	
		/*compare the InfoLine return true if meets the condition*/
		
		/*create a null table*/
		static public void createTable(Table tableInfo){
			try{	
				 String filename=tableInfo.tableName+".table";
				 PrintWriter out = new PrintWriter( new BufferedWriter(new FileWriter(new File(filename))));
				 out.close();
			}catch(Exception e){
				System.err.println(e.getMessage());
				System.err.println("create table failure");
			}
			System.out.println("create table success");
		}
		
	public static void delete(Table tableInfo) {
			String filename = tableInfo.tableName + ".table";
			try{
				File file = new File(filename);
				FileWriter fw=new FileWriter(file);
			
				fw.write("");
				fw.flush();
				fw.close();
				
				Vector<String> allIndex=CatalogManager.relativaIndex(tableInfo.tableName);
				for(int i=0;i<allIndex.size();i++){	
					Index inx2 = CatalogManager.getIndex(allIndex.elementAt(i));
					
					IndexManager.dropIndex(allIndex.elementAt(i));
					CatalogManager.Drop_Index(allIndex.elementAt(i));
					BufferManager.dropTable(allIndex.elementAt(i)+".index");
				
					CatalogManager.Create_Index(inx2);
					IndexManager.createIndex(tableInfo,inx2);
				}			
				
			}catch(Exception   e){
				System.err.println(e.getMessage());
				System.err.println("delete table error");
			}
			BufferManager.dropTable(filename);
			System.out.println("You have deleted "+filename+" out");
		}
	
	public static void delete(Table tableInfo,Vector<Condition> conditions) throws Exception{
			String filename=tableInfo.tableName+".table";    
			int count=0;
			
			Vector<String> allIndex=CatalogManager.relativaIndex(tableInfo.tableName);
			
			for(int blockOffset=0; blockOffset< tableInfo.blockNum; blockOffset++){
				BufferBlock block = BufferManager.readBlock(filename,blockOffset);
				
				for(int offset =0; offset < block.recordNum; offset++){
					int position = offset*tableInfo.recordLength; 
					//get every record line(may across blocks)
					byte[] RecordLine =block.getBytes(position, tableInfo.recordLength);
					Record red=splitRecord(tableInfo,RecordLine);
					
					if(Compare(tableInfo,red,conditions)){   
						BufferManager.deleteValues(blockOffset,position,tableInfo);
						offset--;
						count++; 
						block.recordNum--;
						//delete respective  values in the record in the index tree
						for(int i=0;i<allIndex.size();i++){	
							Index inx2 = CatalogManager.getIndex(allIndex.elementAt(i));	
							IndexManager.deleteKey(inx2,red.columns.get(inx2.column));			
						}
						
					}
				}
			}
			
			System.out.println("delete success");
			System.out.println("Delete "+count+" records in total");
		}
/*drop the table file and all the index files related to this table*/	
	public static void dropTable(String tableName){
		String filename = tableName + ".table";
		File file = new File(filename);
		
		try{
			if(file.exists())
				if(file.delete())   
					System.out.println("The file has been delete");
			else
				System.out.println(""+filename+" not found");
			
			Vector<String> allIndex=CatalogManager.relativaIndex(tableName);
			for(int i=0;i<allIndex.size();i++){
				String indexname = allIndex.elementAt(i) + ".index";
				File indexfile = new File(indexname);
				if(indexfile.exists())
					if(indexfile.delete())   
						System.out.println("index"+indexname+"deleted");
			}			
        }catch(Exception   e){
            System.err.println(e.getMessage());
            System.err.println("error when drop table!");
        }
			
	}
/*check if there is at least one record meet the needs*/
	public static boolean exist(Table tableInfo, Vector<Condition> conditions) throws UnsupportedEncodingException {
		String filename=tableInfo.tableName+".table";    		
		
		for(int blockOffset=0; blockOffset< tableInfo.blockNum; blockOffset++){
			
			BufferBlock block = BufferManager.readBlock(filename,blockOffset);
			for(int offset =0; offset < block.recordNum; offset++){
				int position = offset*tableInfo.recordLength; 
				byte[] RecordLine =block.getBytes(position, tableInfo.recordLength);				
				Record red=splitRecord(tableInfo,RecordLine);
				if(Compare(tableInfo,red,conditions)) 
					return true;
			}
		}
		
		return false;
	}
/*complete the whole insert operation including the index*/
	public static void insertValue(Table tableInfo,Record InfoLine) throws Exception{
		String filename=tableInfo.tableName+".table";
		
		BufferBlock blk=BufferManager.getInsertPosition(tableInfo,filename); 
		if(blk==null){
			blk=BufferManager.createBlock(filename, tableInfo.blockNum);
			tableInfo.blockNum++;
		}		
		//insert the record line into the proper position
		int pos=blk.recordNum*tableInfo.recordLength;
		for(int i=0;i<InfoLine.columns.size();i++){
			blk.setBytes(pos,InfoLine.columns.get(i));	
			pos+=tableInfo.attrlist.get(i).length;
	    }
		//insert every value into the index 
		Vector<String> allIndex=CatalogManager.relativaIndex(tableInfo.tableName);
		for(int i=0;i<allIndex.size();i++){	
			Index inx2 = CatalogManager.getIndex(allIndex.elementAt(i));	
			IndexManager.insertKey(inx2,InfoLine.columns.get(inx2.column), blk.blockOffset, blk.recordNum);		
		}
		
		blk.recordNum++;	
		System.out.println("insert success!");
	}
	/*show all the records related the table(may across blocks)*/
	public static void select(Table tableInfo) throws UnsupportedEncodingException{
		String filename=tableInfo.tableName+".table";    
		Data datas=new Data();
		
		for(int blockOffset=0; blockOffset< tableInfo.blockNum; blockOffset++){
			BufferBlock block = BufferManager.readBlock(filename,blockOffset);
			for(int offset =0; offset < block.recordNum; offset++){
				int position = offset*tableInfo.recordLength; 
				byte[] RecordLine =block.getBytes(position, tableInfo.recordLength);
				Record red=splitRecord(tableInfo,RecordLine); 
				row Row=bytesToString(tableInfo,red);
				datas.Lines.add(Row);		
			}
		}
		showDatas(datas);		
	}
	/*select the attributes chosen and  show the records(may cross blocks)*/	
	static public void select(Table tableInfo,selectAttribute selections) throws UnsupportedEncodingException{
		String filename=tableInfo.tableName+".table";  
		Data datas=new Data();
		
		for(int blockOffset=0; blockOffset< tableInfo.blockNum; blockOffset++){
			BufferBlock block = BufferManager.readBlock(filename,blockOffset);
			for(int offset =0; offset < block.recordNum; offset++){
				int position = offset*tableInfo.recordLength; 
				byte[] RecordLine =block.getBytes(position, tableInfo.recordLength);
				Record red=splitRecord(tableInfo,RecordLine);
				Record rred=red.selectRecord(tableInfo,selections);  
				row Row=bytesToString(tableInfo,rred,selections);
				datas.Lines.add(Row);
			}
		}
		
		showDatas(datas);	
	}

	/*select and show the records(certain attributes) which meets the conditions(may cross blocks)*/
	public static void select(Table tableInfo,selectAttribute selections,Vector<Condition> conditions) throws UnsupportedEncodingException{
		String filename=tableInfo.tableName+".table";  
		Data datas=new Data();
		
		for(int blockOffset=0; blockOffset< tableInfo.blockNum; blockOffset++){
			BufferBlock block = BufferManager.readBlock(filename,blockOffset);
			for(int offset =0; offset < block.recordNum; offset++){
				int position = offset*tableInfo.recordLength; 
				byte[] RecordLine =block.getBytes(position, tableInfo.recordLength);
				Record red=splitRecord(tableInfo,RecordLine);
				if(Compare(tableInfo,red,conditions)){  
					Record rred=red.selectRecord(tableInfo, selections); //??
					row Row=bytesToString(tableInfo,rred,selections);//??
					datas.Lines.add(Row);	
				}
			}
		}
		
		showDatas(datas);	
	}
	
/*delete the records from the table which meets the condition*/
	
	/*select and show the records which meets the conditions(my cross blocks)*/
	public static void select(Table tableInfo,Vector<Condition> conditions) throws UnsupportedEncodingException{
		String filename=tableInfo.tableName+".table";    
		Data datas=new Data();
		
		for(int blockOffset=0; blockOffset< tableInfo.blockNum; blockOffset++){
			BufferBlock block = BufferManager.readBlock(filename,blockOffset);
			for(int offset =0; offset < block.recordNum; offset++){
				int position = offset*tableInfo.recordLength; 
				byte[] RecordLine =block.getBytes(position, tableInfo.recordLength);				
				Record red=splitRecord(tableInfo,RecordLine);
				if(Compare(tableInfo,red,conditions)){ 
					row Row=bytesToString(tableInfo,red);
					datas.Lines.add(Row);	
				}
			}
		}
		
		showDatas(datas);	
	}
	/*select one record according to the offset info and show the record*/
	public static void  selectFromIndex(Table tableInfo,offsetInfo off) throws UnsupportedEncodingException{
		String filename=tableInfo.tableName+".table";   
		Data datas=new Data();
		
		if(off==null){
			System.out.println("can't find from index");
			return;
		}
		
		BufferBlock block=BufferManager.readBlock(filename, off.offsetInfile);
		int position = off.offsetInBlock*tableInfo.recordLength; 
		
		byte[] RecordLine =block.getBytes(position, tableInfo.recordLength);
		Record red=splitRecord(tableInfo,RecordLine); 
		row Row=bytesToString(tableInfo,red);
		datas.Lines.add(Row);		
		showDatas(datas);  
	}
	/*select the needed attributes in a record according to the offset info and show it*/
	public static void  selectFromIndex(Table tableInfo,selectAttribute selections,offsetInfo off) throws UnsupportedEncodingException{
		String filename=tableInfo.tableName+".table";   
		Data datas=new Data();

		if(off==null){
			System.out.println("can't find from index");
			return;
		}		
		BufferBlock block=BufferManager.readBlock(filename, off.offsetInfile);
		int position = off.offsetInBlock*tableInfo.recordLength; 
		byte[] RecordLine =block.getBytes(position, tableInfo.recordLength);		
		Record red=splitRecord(tableInfo,RecordLine);
		
		Record rred=red.selectRecord(tableInfo,selections); 
		row Row=bytesToString(tableInfo,rred,selections);
		datas.Lines.add(Row);	
		
		showDatas(datas);
	}
	
	/*show the data added into the datas */
	public static void showDatas(Data datas){
		if(datas.Lines.size()==0){
			System.out.println("The query result is empty");
			return;
		}
		
		for(int i=0;i<datas.Lines.size();i++){
			for(int j=0;j<datas.Lines.get(i).columns.size();j++){
				System.out.print(datas.Lines.get(i).columns.get(j)+"\t");  
			}
			System.out.println();
		}
	}
  /*split the recordLine into the returnRecord(vector<[]byte>) according to the tableInfo*/
	private static Record splitRecord(Table tableInfo,byte[] recordLine){
		Record returnRecord=new Record();
		byte[] tmpbyte;
		
		int startpos=0; 
		for(int i=0;i<tableInfo.attriNum;i++){
			tmpbyte=new byte[tableInfo.attrlist.get(i).length];
			for(int j=0;j<tmpbyte.length;j++){
				tmpbyte[j]=recordLine[startpos+j];
			}
			//the valid attribute length does not equal to the stored length
			if(tableInfo.attrlist.get(i).type==CHAR){
				int validlength=0;
				for(;validlength<tableInfo.attrlist.get(i).length && tmpbyte[validlength]!='&';validlength++);
				byte[] tmp; 
                
				//possible?
				if(validlength==tableInfo.attrlist.get(i).length-1 && tmpbyte[validlength]!='&'){
					tmp=new byte[validlength+1];
					for(int j=0;j<validlength+1;j++){
						tmp[j]=tmpbyte[j];
					}
				}	
				else{
					tmp=new byte[validlength];
					for(int j=0;j<validlength;j++){
						tmp[j]=tmpbyte[j];
					}
				}
				
				returnRecord.columns.add(tmp);
			}		
			else returnRecord.columns.add(tmpbyte);
			
			startpos+=tableInfo.attrlist.get(i).length;
		}
		
		return returnRecord;
	}
	
}
