package Minisql_final;

import java.io.UnsupportedEncodingException;
import java.util.*;

import Minisql_final.Structure.*;

public class API {
	
	public static final int CHAR = 3;
	
	public static final int FLOAT = 2;
	
	public static final int INT = 1;
	
	public static String SQL;

	
	public static void API_Moudle(String a) throws Exception{
		
		SQL=a.trim();//
		String array1[]=SQL.split(" ", -1);
		if(array1[0].equals("00")){//create database
			
		}
		else if(array1[0].equals("01")){//delete database
			
		}
		else if(array1[0].equals("10")){//create table
			boolean flag=false;
			Vector<Attribute> attributes=new Vector< Attribute>(); 
			int length=array1.length;
			Table tb1=new  Table();
			String tbname=array1[1];
			tb1.tableName=tbname;
			tb1.attrlist=new ArrayList<Attribute>(length/3-1);
			for(int i4=0;i4<length/3-1;i4++){
				tb1.attrlist.add(new Attribute());
			}
			int j=0;
			int i;
			for(i=2;(array1[i].equalsIgnoreCase("#")==false)&&(array1[i].equalsIgnoreCase(";")==false);)
			{
				tb1.attrlist.get(j).name=array1[i++];
				if(array1[i].toLowerCase().equals("float")){
					tb1.attrlist.get(j).length=4;
					tb1.attrlist.get(j).type=2;
				}
				else if(array1[i].toLowerCase().equals("int")){
					tb1.attrlist.get(j).length=4;
					tb1.attrlist.get(j).type=1;
				}
				
				else/*((Integer.parseInt(array1[i])<=9)&&(Integer.parseInt(array1[i])>=0))*/{
					tb1.attrlist.get(j).type=3;
					tb1.attrlist.get(j).length=Integer.parseInt(array1[i]);
				}
				i++;
				if(array1[i].equals("1"))
					tb1.attrlist.get(j).unique=true;
				else
					tb1.attrlist.get(j).unique=false;
				j++;
				i++;
			}
			if(array1[i].equalsIgnoreCase("#")==true)//
				flag=true;
			i++;
			tb1.attriNum=j;
			if(flag==true)
			{
				tb1.primaryKey=array1[i];
				for(int i3=0;i3<tb1.attriNum;i3++){
					//for(int i4=0;i4<j2;i4++){
						if(tb1.attrlist.get(i3).name.equals(tb1.primaryKey)){
							tb1.attrlist.get(i3).isPrimeryKey=true;
							tb1.attrlist.get(i3).unique=true;
						}
						else
							tb1.attrlist.get(i3).isPrimeryKey=false;
					//}
						attributes.add(tb1.attrlist.get(i3));
				}	
			}
			
			
			tb1.blockNum=1;
			tb1.recordLength=0;
			for(int i2=0;i2<j;i2++)
				tb1.recordLength+=tb1.attrlist.get(i2).length;
			tb1.recordLength=tb1.recordLength;
			tb1.maxRecordsPerBlock=4096/tb1.recordLength;
			
			CatalogManager.Create_Table(tb1);
			RecordManager.createTable(tb1);
			
			if(flag==true)
			{
				for(int i3=0;i3<tb1.attriNum;i3++){
					//for(int i4=0;i4<j2;i4++){
						if(tb1.attrlist.get(i3).name.equals(tb1.primaryKey)){
							Index inx2 = new Index();
							inx2.indexName=tbname+"-primary-idx";
							inx2.tableName=tbname;
							inx2.column=i3;
							inx2.columnLength=tb1.attrlist.get(i3).length;
							inx2.rootNum=0;
							inx2.blockNum=0;
							
							CatalogManager.Create_Index(inx2);
							IndexManager.createIndex(tb1,inx2);
							break;
						}	
				}	
			}
			

		}
		else if(array1[0].equals("11")){//delete table
			String tbname="";
			tbname=array1[1];
			RecordManager.dropTable(tbname);
			CatalogManager.Drop_Table(tbname);	
			BufferManager.dropTable(tbname+".table");
			
		}
		else if(array1[0].equals("20")){//create index
			Index inx2 = new Index();
			inx2.indexName=array1[1];
			Table tb2=CatalogManager.getTable(array1[2]);
			inx2.tableName=tb2.tableName;
			String attr=array1[3];
			for(int i7=0;i7<tb2.attriNum;i7++){
				if(tb2.attrlist.get(i7).name.equals(attr)){
					inx2.column=i7;
					inx2.columnLength=tb2.attrlist.get(i7).length;
					break;
				}
			}
			inx2.rootNum=0;
			inx2.blockNum=0;
			CatalogManager.Create_Index(inx2);
			IndexManager.createIndex(tb2,inx2);
			
		}
		else if(array1[0].equals("21")){//delete index
			IndexManager.dropIndex(array1[1]);
			CatalogManager.Drop_Index(array1[1]);
			BufferManager.dropTable(array1[1]+".index");
		}
		else if(array1[0].equals("30")){//select
			int flage=0;
			int codi=0;
			String tbname=array1[2];
			int length=array1.length;
			 Table tb2=new  Table();
			tb2=CatalogManager.getTable(tbname);
			Vector< Condition> conditions=new Vector< Condition>();
			 Index indexinfo=new  Index();
			 offsetInfo ofsi=new  offsetInfo();
			Vector<String> attribute=new Vector<String>();
			long start=0; 
			for(int i5=4;i5<length;i5++){
				
				if(array1[i5].equals("#")){
					codi=1;
					i5++;
					 Condition cd=new  Condition();
					char[] shux=array1[i5].toCharArray();
					for(int i9=0;i9<shux.length;i9++){
						
						if(shux[i9]=='='){
							String array2[]=array1[i5].split("=", -1);
							cd.op=Comparison.Eq;
							cd.value=array2[1];
							for(int i0=0;i0<tb2.attriNum;i0++){
								if(array2[0].equals(tb2.attrlist.get(i0).name)){
									cd.columnNum=i0;
									break;
								}
							}
							indexinfo=CatalogManager.getIndexfromTable(tbname,array2[0]);
							if((indexinfo!=null)&&(i5+2==length)){
								// index exists and only 1 conditions
								//cd.value需要补齐，但是现在还没有补齐
								
								start= System.currentTimeMillis();
								ofsi=IndexManager.searchEqual(indexinfo, stringToBytes(tb2.attrlist.get(cd.columnNum),cd.value));//indexinfo��һ�����飬���β���һ��Index���͵�ֵ
								flage=1;
							}
							else
								conditions.add(cd);
							
							break;
						}
						else if((shux[i9]=='>')&&(shux[i9+1]!='=')){
							String array2[]=array1[i5].split(">", -1);
							cd.op=Comparison.Gt;
							cd.value=array2[1];
							for(int i0=0;i0<tb2.attriNum;i0++){
								if(array2[0].equals(tb2.attrlist.get(i0).name)){
									cd.columnNum=i0;
								}
							}
							conditions.add(cd);
							break;
						}
						else if((shux[i9]=='>')&&(shux[i9+1]=='=')){
							String array2[]=array1[i5].split(">=", -1);
							cd.op=Comparison.Ge;
							cd.value=array2[1];
							for(int i0=0;i0<tb2.attriNum;i0++){
								if(array2[0].equals(tb2.attrlist.get(i0).name)){
									cd.columnNum=i0;
								}
							}
							conditions.add(cd);
							break;
						}
						else if((shux[i9]=='<')&&(shux[i9+1]=='>')){
							String array2[]=array1[i5].split("<>", -1);
							cd.op=Comparison.Ne;
							cd.value=array2[1];
							for(int i0=0;i0<tb2.attriNum;i0++){
								if(array2[0].equals(tb2.attrlist.get(i0).name)){
									cd.columnNum=i0;
								}
							}
							conditions.add(cd);
							break;
						}
						else if((shux[i9]=='<')&&(shux[i9+1]=='=')){
							String array2[]=array1[i5].split("<=", -1);
							cd.op=Comparison.Le;
							cd.value=array2[1];
							for(int i0=0;i0<tb2.attriNum;i0++){
								if(array2[0].equals(tb2.attrlist.get(i0).name)){
									cd.columnNum=i0;
								}
							}
							conditions.add(cd);
							break;
						}
						else if((shux[i9]=='<')){
							String array2[]=array1[i5].split("<", -1);
							cd.op=Comparison.Ls;
							cd.value=array2[1];
							for(int i0=0;i0<tb2.attriNum;i0++){
								if(array2[0].equals(tb2.attrlist.get(i0).name)){
									cd.columnNum=i0;
								}
							}
							conditions.add(cd);
							break;
						}
						
						
					}
				}
			}
			
			if(array1[4].equals("*")){
				
				if(flage==1){
					RecordManager.selectFromIndex(tb2,ofsi);
					System.out.println("time cost with index: " + (System.currentTimeMillis() - start));  
				}
				else if(codi==1){
					start= System.currentTimeMillis();
					RecordManager.select(tb2,conditions);
					System.out.println("time cost without index: " + (System.currentTimeMillis() - start));  
				}
				else{
					start= System.currentTimeMillis();
					RecordManager.select(tb2);
					System.out.println("time cost without index: " + (System.currentTimeMillis() - start));  
				}
			}
			else{

				
				 selectAttribute sa1=new  selectAttribute();
				sa1.columns=new Vector<Integer> ();
				
				for(int i=4;i<array1.length&&(array1[i].equals("#"))==false;i++){
					attribute.add(array1[i]);
					for(int i2=0;i2<tb2.attriNum;i2++){
						if(tb2.attrlist.get(i2).name.equals(array1[i2])){
							sa1.columns.add(i2);
						}
					}
				}
				String[] attris=new String[attribute.size()];
				for(int i7=0;i7<attribute.size();i7++){
					attris[i7]=attribute.get(i7);
				}
				if(flage==1){
					RecordManager.selectFromIndex(tb2,sa1,ofsi);
					System.out.println("time cost with index: " + (System.currentTimeMillis() - start));  
				}
				else if(codi==1){
					start= System.currentTimeMillis();
					RecordManager.select(tb2,sa1,conditions);
					System.out.println("time cost without index: " + (System.currentTimeMillis() - start));  
				}
				else{
					start= System.currentTimeMillis();
					RecordManager.select(tb2,conditions);
					System.out.println("time cost without index: " + (System.currentTimeMillis() - start));  
				}
			}
		}
		else if(array1[0].equals("40")){//insert
			String array2[]=array1[3].split(",", -1);
			int length=array2.length;
			String tbname=array1[1];
			Vector< Condition> conditions=new Vector< Condition>();
			 Table tb2=new  Table();
			 Record rc=new  Record();
			 
			tb2=CatalogManager.getTable(tbname);
			if(tb2==null){
				System.out.print("File " + tbname +" not found");
				return ;
			}
			int exist = 0;
			for(int i=0;i<length;i++){
				if(array2[i].equals(" "))
					array2[i]="";
				if(tb2.attrlist.get(i).unique){
					Condition cd=new  Condition();
					cd.op=Comparison.Eq;
					cd.value=array2[i];
					cd.columnNum=i;

					conditions.add(cd);
					if(RecordManager.exist(tb2,conditions)){
					
						System.out.println(tb2.attrlist.get(i).name+"="+array2[i]+" is existed!");
						exist=1;
						break;
					}
				}
				//char[] value=array2[i].toCharArray();
				//for(int i3=0;i3<(tb2.attrlist[i].length-value.length);i3++)//����
				//	array2[i]+="&";
				byte[] tmpBytes=stringToBytes(tb2.attrlist.get(i),array2[i]);	
				rc.columns.add(tmpBytes);
			}
			if(exist==0){
				RecordManager.insertValue(tb2,rc);
			}
			
		}
		else if(array1[0].equals("41")){//delete record
			Table tb2=new  Table();
			String tbname=array1[1];
			tb2=CatalogManager.getTable(tbname);
			int length=array1.length;
			
			Vector< Condition> conditions=new Vector< Condition>();
			for(int i5=2;i5<length;i5++){
				
				if(array1[i5].equals("#")){
					i5++;
					 Condition cd=new  Condition();
					char[] shux=array1[i5].toCharArray();
					for(int i9=0;i9<shux.length;i9++){
						
						if(shux[i9]=='='){
							String array2[]=array1[i5].split("=", -1);
							cd.op=Comparison.Eq;
							cd.value=array2[1];
							for(int i0=0;i0<tb2.attriNum;i0++){
								if(array2[0].equals(tb2.attrlist.get(i0).name)){
									cd.columnNum=i0;
								}
							}
							conditions.add(cd);
							
							break;
						}
						else if((shux[i9]=='>')&&(shux[i9+1]!='=')){
							String array2[]=array1[i5].split(">", -1);
							cd.op=Comparison.Gt;
							cd.value=array2[1];
							for(int i0=0;i0<tb2.attriNum;i0++){
								if(array2[0].equals(tb2.attrlist.get(i0).name)){
									cd.columnNum=i0;
								}
							}
							conditions.add(cd);
							break;
						}
						else if((shux[i9]=='>')&&(shux[i9+1]=='=')){
							String array2[]=array1[i5].split(">=", -1);
							cd.op=Comparison.Ge;
							cd.value=array2[1];
							for(int i0=0;i0<tb2.attriNum;i0++){
								if(array2[0].equals(tb2.attrlist.get(i0).name)){
									cd.columnNum=i0;
								}
							}
							conditions.add(cd);
							break;
						}
						else if((shux[i9]=='<')&&(shux[i9+1]!='=')){
							String array2[]=array1[i5].split("<", -1);
							cd.op=Comparison.Ls;
							cd.value=array2[1];
							for(int i0=0;i0<tb2.attriNum;i0++){
								if(array2[0].equals(tb2.attrlist.get(i0).name)){
									cd.columnNum=i0;
								}
							}
							conditions.add(cd);
							break;
						}
						else if((shux[i9]=='<')&&(shux[i9+1]=='=')){
							String array2[]=array1[i5].split("<=", -1);
							cd.op=Comparison.Ls;
							cd.value=array2[1];
							for(int i0=0;i0<tb2.attriNum;i0++){
								if(array2[0].equals(tb2.attrlist.get(i0).name)){
									cd.columnNum=i0;
								}
							}
							conditions.add(cd);
							break;
						}
						else if((shux[i9]=='<')&&(shux[i9+1]=='>')){
							String array2[]=array1[i5].split("<>", -1);
							cd.op=Comparison.Ne;
							cd.value=array2[1];
							for(int i0=0;i0<tb2.attriNum;i0++){
								if(array2[0].equals(tb2.attrlist.get(i0).name)){
									cd.columnNum=i0;
								}
							}
							conditions.add(cd);
							break;
						}
					}
				}
			}
			if(length==4)
			{
				RecordManager.delete(tb2);
				//CatalogManager.setTableBlockNum(tb2,1);	
			}	
			else{
				RecordManager.delete(tb2,conditions);
			}
			
		}
		else if(array1[0].equals("50")){//exit
			System.out.println("Exit.");
		}
		else if(array1[0].equals("99")){
			
		}
		else if(array1[0].equals("70")){//help
			
		}
		
	}
	
	static public byte[] stringToBytes(Attribute attr,String tmpString) throws UnsupportedEncodingException{
		
		byte[] tmpbyte=new byte[attr.length];
		
		switch(attr.type){
			case CHAR:
				byte[] tmpb=tmpString.getBytes("ISO-8859-1");
				int i=0;
				for(;i<tmpb.length;i++){
					tmpbyte[i]=tmpb[i];
				}
				for(;i<attr.length;tmpbyte[i++]='&');
				
				break;
			case INT:
				tmpbyte=new byte[4];
				int intvalue1=Integer.valueOf(tmpString).intValue();
				for(int j=0;j<4;j++){
					tmpbyte[j]=(byte)(intvalue1>>8*(3-j)&0xFF);
				}
				break;
			case FLOAT:
				tmpbyte=new byte[4];
				float flvalue2=Float.valueOf(tmpString).floatValue();
				int l = Float.floatToIntBits(flvalue2); 
				for (int j = 0; j < 4; j++) { 
					tmpbyte[j] = new Integer(l).byteValue(); 
					l = l >> 8; 
				}
				break;
		}
		
		return tmpbyte;
	}
}