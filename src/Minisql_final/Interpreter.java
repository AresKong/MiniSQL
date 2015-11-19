package Minisql_final;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.InputStreamReader;


public class Interpreter {

	static String sql;

	static void add_Attribution(StringBuffer SQL,int begin)
	{
		int start = filter(SQL,begin);
		String att;
		String word;
		int end;
		while((end = SQL.indexOf(",",start)) != -1)
		{
			if(sql.equals("99"))
				break;
			att = SQL.substring(start, end);
			end = att.indexOf(' '); // get the attribution name 
			if(end == -1)
			{
				sql = "99";
				System.out.println("Attribution name missing!Please check your input!!\r\n");
				return;
			}
			word = SQL.substring(start, start+end);
			start = end +start+ 1;
			if(isValid(word))
			{
				word = word.trim();
				sql += (word+" ");
				start = filter(SQL,start);
				if((SQL.indexOf(" ",start)<SQL.indexOf(",",start))&&(SQL.indexOf(" ", start)>=0))
					end = SQL.indexOf(" ",start);
				else
					end = SQL.indexOf(",",start);
				if(end == -1)
				{
					sql = "99";
					System.out.println("Attribution type missing!Please check your input!!\r\n");
					return;
				}
				word = SQL.substring(start, end);
				if(word.equals("int"))
				{
					sql += "int ";
					start = end ;
				}
				else if(word.equals("float"))
				{
					sql += "float ";
					start = end ;
				}
				else if(word.startsWith("char"))
				{
					int s,e;
					s = SQL.indexOf("(",start);
					e = SQL.indexOf(")",start);
					if(s==-1||e==-1)
					{
						sql="99";
						System.out.println("\""+word+"\" is not a valid attribution type! "+"Please check your input!!\r\n");
						return;
					}
					else
					{
						String num = SQL.substring(s+1,e);
						if(isValidNum(num))
						{
							num = num.trim();
							sql += (num+" ");
							start = e + 1;
						}
						else
						{
							sql="99";
							System.out.println("\""+num +"\""+" is not a valid number!\r\n");
							return;
						}
					}
				}
				else
				{
					sql="99";
					System.out.println("\""+word +"\""+" is not a valid variable type!\r\n");
					return;
				}
				//is unique
				if(!sql.equals("99"))
				{
					end = SQL.indexOf(",",start);
					if(end == -1)
					{
						sql = "99";
						System.out.println("Attribution format error!Please check your input!!\r\n");
						return;
					}
					word = SQL.substring(start, end);
					if(word.trim().isEmpty())
						sql+="0 ";
					else
					{
						if(isValid(word))
						{
							word = word.trim();
							if(word.equals("unique"))
								sql+="1 ";
							else
							{
								sql="99";
								System.out.println("\""+word+"\""+" is not a key word!\r\n");
								return;
							}
						}
						else
						{
							sql="99";
							System.out.println("\""+word+"\""+" is not a valid key word!\r\n");
							return;
						}
					}
				}
			}
			else
			{
				sql="99";
				System.out.println("\""+word+"\""+" is not a valid attribution name!\r\n");
				return;
			}
			start = filter(SQL,end+1);
		}
		if(!sql.equals("99"))// the last attribution
		{
			end = SQL.lastIndexOf(")");
			if(end == -1)
			{
				sql = "99";
				System.out.println("Commamd lacks \")\"!Please check your input!!\r\n");
				return;
			}
			att = SQL.substring(start, end);
			end = SQL.indexOf(" ",start);
			if(end == -1)
			{
				sql = "99";
				System.out.println("last line attribution format error!!Please check your input!!\r\n");
				return;
			}
			word = SQL.substring(start, end);
			start = end + 1;
			if(isValid(word))
			{
				word = word.trim();
				//is primary
				if(word.equals("primary"))
				{
					start = filter(SQL,start);
					end = SQL.indexOf(" ",start);
					if(end == -1)
					{
						end = SQL.indexOf("(",start);
						if(end == -1)
						{
							sql = "99";
							System.out.println("Commamd lacks \"(\"!Please check your input!!\r\n");
							return;
						}
					}
					word = SQL.substring(start, end);
					if(word.startsWith("key"))
					{
						if(word.endsWith("key"))
							start=end;
						start = SQL.indexOf("(", start);
						end = SQL.indexOf(")",start);
						if(start == -1 || end == -1)
						{
							sql="99";
							System.out.println("last line attribution format error!Please check your input!!\r\n");
							return;
						}
						else
						{
							word = SQL.substring(start+1, end);
							if(isValid(word))
							{
								word = word.trim();
								sql += ("# "+ word+" ; ");
							}
							else
							{
								sql = "99";
								System.out.println(word+" is not a valid word!\r\n");
								return;
							}
						}
					}
					else
					{
						sql="99";
						System.out.println(word+" is not a valid word!\r\n");
						return;
					}
				}
				else// not primary key
				{
					sql += (word+" ");
					start = filter(SQL,start);
					if((SQL.indexOf(" ",start)<SQL.indexOf(")",start))&&(SQL.indexOf(" ",start)>0))
						end = SQL.indexOf(" ",start);
					else
						end = SQL.indexOf(")",start);
					if(end == -1)
					{
						sql = "99";
						System.out.println("last line attribution format error!Please check your input!!\r\n");
						return;
					}
					word = SQL.substring(start, end);
					if(isValid(word))
						word = word.trim();
					else
					{
						sql = "99";
						System.out.println(word+" is not a valid type!\r\n");
					}
					if(word.equals("int"))
					{
						sql += "int ";
						start = end ;
					}
					else if(word.equals("float"))
					{
						sql += "float ";
						start = end ;
					}
					else if(word.startsWith("char"))
					{
						int s,e;
						s = SQL.indexOf("(",start);
						e = SQL.indexOf(")",start);
						if(s==-1||e==-1)
						{
							sql="99";
							System.out.println("\""+word+"\" is not a valid attribution type! "+"Please check your input!!\r\n");
							return;
						}
						else
						{
							String num = SQL.substring(s+1,e);
							if(isValidNum(num))
							{
								num = num.trim();
								sql += (num+" ");
								start = e + 1;
							}
							else
							{
								sql="99";
								System.out.println(num +" is not a valid number!\r\n");
								return;
							}
						}
					}
					else
					{
						sql="99";
						System.out.println(word+" is not a valid variable type!\r\n");
						return;
					}
					//is unique
					if(!sql.equals("99"))
					{
						start = end;
						end = SQL.indexOf(")",start);
						if(end == -1)
						{
							sql = "99";
							System.out.println("Last line:commamd lacks \")\"!Please check your input!!\r\n");
							return;
						}
						word = SQL.substring(start, end);
						if(word.trim().isEmpty())
							sql+="0 ; ";
						else
						{
							if(isValid(word))
							{
								word = word.trim();
								if(word.equals("unique"))
									sql+="1 ";
								else
								{
									sql="99";
									System.out.println("\""+word+"\""+" is not a key word!\r\n");
									return;
								}
							}
							else
							{
								sql="99";
								System.out.println("\""+word+"\""+" is not a valid key word!\r\n");
								return;
							}
						}
					}
				}
			}
		}
	}
	
	static void create_case(StringBuffer SQL,int begin)
	{
		int start = filter(SQL,begin);
		int end;
		String word;
		
		//get the second key word
		end = SQL.indexOf(" ",start);
		if(end == -1)
		{
			sql = "99";
			System.out.println("UNKNOW commamd!Please check your input!!\r\n");
			return;
		}
		word = SQL.substring(start, end);
		start = end + 1;
		switch(word)
		{
		case "database":create_database(SQL,start);break;
		case "table":create_table(SQL,start);break;
		case "index":create_index(SQL,start);break;
		default:sql="99";
				System.out.println("\"create "+word+"\""+" is not an internal command!\r\n");break;//����޴�����Ĵ�����ʾ
		}
	}
	
	static void create_database(StringBuffer SQL,int begin)
	{
		int end;
		String word;
		sql="00 ";
		end = SQL.lastIndexOf(";");
		word = SQL.substring(begin, end);
		if(isValid(word) == true)
		{
			word = word.trim();
			sql+=(word+" ; ");
			return;
		}
		else
		{
			sql="99";
			System.out.println("\""+word +"\""+ " is not a valid database name!\r\n");
			return;
		}
	}

	static void create_index(StringBuffer SQL,int begin)
	{
		int start = filter(SQL,begin);
		int end = SQL.indexOf(" ", start);
		if(end == -1)
		{
			sql = "99";
			System.out.println("Index name missing!Please check your input!!\r\n");
			return;
		}
		String word = SQL.substring(start, end);
		if(isValid(word))//index����Ч
		{
			word = word.trim();
			if(CatalogManager.getIndex(word) != null)
			{
				sql = "99";
				System.out.println("Index \""+word+"\" has been created!\r\n");
				return;
			}
			sql = ("20 "+word+" ");
			start = filter(SQL,end);
			end = SQL.indexOf(" ", start);
			if(end == -1)
			{
				sql = "99";
				System.out.println("Incorrect use of \"create index\" commamd!Please check your input!!\r\n");
				return;
			}
			word = SQL.substring(start, end);
			 if(word.equals("on"))
			 {
				 //find the table name
				 start = filter(SQL,end);
				 end = SQL.indexOf("(");
				 if(end == -1)
				 {
					 sql = "99";
					 System.out.println("Table name missing!Please check your input!!\r\n");
					 return;
				 }
				 else
				 {
					 word = SQL.substring(start,end);
					 if(isValid(word))
					 {
						 word = word.trim();
						 if(CatalogManager.getTable(word) != null)//is table existing
						 {
							 sql += (word + " ");
							 start = filter(SQL,end + 1);
							 end = SQL.indexOf(")", start);
							 if(end == -1)
								{
									sql = "99";
									System.out.println("Commamd lacks \")\"!Please check your input!!\r\n");
									return;
								}
							 String wordatt = SQL.substring(start, end);
							 if(isValid(wordatt))
							 {
								 wordatt = wordatt.trim();
								 //is attribution existing
								 if(CatalogManager.isAttribution(word,wordatt))
								 {
									 sql += (wordatt+" ; ");
									 return;
								 }
								 else
								 {
									 sql = "99";
									 System.out.println("Error:"+"\""+wordatt+"\""+"is not a attribution of "+word+"!\r\n");
									 return;
								 }
							 }
							 else
							 {
								 sql = "99";
								 System.out.println("\""+wordatt+"\"" +" is not a valid attribution name!\r\n");
								 return;
							 }
						 }					 
						 else
						 {
							 sql = "99";
							 System.out.println("\""+word+"\""+" is not a existing table!\r\n");
							 return;
						 }
					 }
					 else
					 {
						 sql = "99";						 
						 System.out.println("\""+word+"\""+" is not a valid word!\r\n");
						 return;
					 }
				 }
			 }
			 else
			 {
				 sql = "99";
				 System.out.println("Incorrect use of \"create index\" commamd!Please check your input!!\r\n"); 
				 return;
			 }
		}
		else
		{
			sql = "99";
			System.out.println("\""+word+"\"" + " is not a valid word!\r\n");
			return;
		}
	}

	static void create_table(StringBuffer SQL,int begin)
	{
		int start,end;
		String word;
		sql="10 ";
		end = SQL.indexOf("(");
		if(end == -1)
		{
			sql="99";
			System.out.println("Can't find the attribution!Please check your input!!\r\n");
			return;
		}
		else
		{
			word = SQL.substring(begin, end);
			start = end + 1;
			if(isValid(word) == true)
			{
				word = word.trim();
				if(CatalogManager.getTable(word) != null)
				{
					sql="99";
					System.out.println("table: \""+word +"\" has been created in the database!\r\n");
					return;
				}
				else
				{
					sql+=(word + " ");
					add_Attribution(SQL,start);
					return;
				}
			}
			else
			{
				sql="99";
				System.out.println("\""+word+"\"" + " is not a valid table name!\r\n");
				return;
			}
		}
	}

	static void delete(StringBuffer SQL,int begin)
	{
		sql = "41 ";
		String tab = null;
		//get the second word
		int start = filter(SQL,begin);
		int end = SQL.indexOf(" ",start);
		if(end == -1)
		{
			sql = "99";
			System.out.println("Incorrect use of \"delete\" commamd!Please check your input!!\r\n");
			return;
		}
		String word = SQL.substring(start, end);
		start = end + 1;
		if(word.equals("from"))
		{
			//get the table name
			start = filter(SQL,start);
			end = SQL.indexOf("where", start);
			if(end == -1)
			{
				end = SQL.indexOf(";");
				if(end == -1)
				{
					sql = "99";
					System.out.println("Incorrect use of \"delete\" commamd!Please check your input!!\r\n");
					return;
				}
				word = SQL.substring(start, end);
				tab = SQL.substring(start, end);
				if(isValid(word))
				{
					word = word.trim();
					if(CatalogManager.getTable(word)!=null)
					{
						sql += (word+" ; ");
						return;
					}
					else
					{
						sql = "99";
						System.out.println("Table "+"\""+word+"\""+" has not been created!\r\n");
						return;
					}
				}
				else
				{
					sql = "99";
					System.out.println("\""+word+"\""+" is not a valid table name!\r\n");
					return;
				}
			}
			else// delete condition
			{
				word = SQL.substring(start, end);
				if(isValid(word))
				{
					word = word.trim();
					if(CatalogManager.getTable(word) != null)
					{
						sql += (word+" ");
						start = end +5;
						String t;
						int length ;
						while((end = SQL.indexOf("and", start))!=-1)
						{
							t=SQL.substring(start, end);
							length = t.length()-1;
							while(t.charAt(length)==' ')
								length--;
							t=t.substring(start, length+1);
							t = findCondition(t,word);
							if(t.equals("err"))
							{
								sql = "99";
								System.out.println("Condition is not valid!\r\n");
								return;
							}
							sql += ("# "+t+" ");
							start = end + 3;
						}
						end = SQL.indexOf(";");
						if(end == -1)
						{
							sql = "99";
							System.out.println("Incorrect use of \"delete\" commamd!Please check your input!!\r\n");
							return;
						}
						t=SQL.substring(start, end);
		
						t = t.trim();
						t = findCondition(t,word);
						sql += ("# "+t+" ; ");
						return;
					}
					else
					{
						sql = "99";
						System.out.println("Table "+"\""+word+"\""+" has not been created!\r\n");
						return;
					}
				}
				else
				{
					sql = "99";
					System.out.println("\""+word+"\""+" is not a valid table name!\r\n");
					return;
				}
			}
		}
		else
		{
			sql = "99";
			System.out.println("Lacking the key word \"from\"!Please check your input!");
			return;
		}
	}

	static void drop_case(StringBuffer SQL,int begin)
	{
		int start = filter(SQL,begin);
		int end;
		String word;
		end = SQL.indexOf(" ",start);
		if(end == -1)
		{
			sql = "99";
			System.out.println("UNKNOW commamd!Please check your input!!\r\n");
			return;
		}
		word = SQL.substring(start, end);
		start = end + 1;
		switch(word)
		{
		case "database":drop_database(SQL,start);
						break;
		case "table":drop_table(SQL,start);
					 break;
		case "index":drop_index(SQL,start);
			         break;
		default:sql="99";
				System.out.println("\""+word+"\""+" is not an internal command!\r\n");break;
		}
	}

	static void drop_database(StringBuffer SQL,int begin)
	{
		int end;
		String word;
		sql="01 ";
		end = SQL.lastIndexOf(";");
		word = SQL.substring(begin, end);
		if(isValid(word) == true)
		{
			word = word.trim();
			sql+=(word+" ; ");
			return;
		}
		else
		{
			sql="99";
			System.out.println("\""+word+"\"" + " is not a valid database name!\r\n");
			return;
		}
	}

	static void drop_index(StringBuffer SQL,int begin)
	{
		int end;
		String word;
		sql="21 ";
		end = SQL.lastIndexOf(";");
		word = SQL.substring(begin, end);
		if(isValid(word) == true)
		{
			word = word.trim();
			//is table existing 
			if(CatalogManager.getIndex(word) != null)
			{
				sql+=(word+" ; ");
			}
			else
			{
				sql="99";
				System.out.println("Index: "+"\""+word+"\"" +" has not been created in the database!\r\n");
			}
		}
		else
		{
			sql="99";
			System.out.println("\""+word+"\"" + " is not a valid index name!\r\n");
		}
	}

	static void drop_table(StringBuffer SQL,int begin)
	{
		int end;
		String word;
		sql="11 ";
		end = SQL.lastIndexOf(";");
		word = SQL.substring(begin, end);
		if(isValid(word) == true)
		{
			word = word.trim();
			//is table existing 
			if(CatalogManager.getTable(word) != null)
			{
				sql+=(word+" ; ");
				return;
			}
			else
			{
				sql="99";
				System.out.println("table: "+"\""+word+"\"" +" has not been created in the database!\r\n");
				return;
			}
		}
		else
		{
			sql="99";
			System.out.println(word + " is not a valid table name!\r\n");
			return;
		}
	}

	static void execfile(StringBuffer SQL,int begin) throws Exception
	{
		int start = filter(SQL,begin);
		int end = SQL.indexOf(";",start);
		if(end == -1)
		{
			sql = "99";
			System.out.println("file name error!Please check your input!!\r\n");
			return;
		}
		String word = SQL.substring(start, end);
		int length = word.length()-1;
		while(word.charAt(length) == ' ')
			length--;
		word=word.substring(0, length+1);
		String lineTxt = null;
		String f = new String();
		try {
			File file = new File(word);
			if (file.isFile() && file.exists()) { 
				InputStreamReader read = new InputStreamReader(new FileInputStream(file));
				BufferedReader bufferedReader = new BufferedReader(read);
				while ((lineTxt = bufferedReader.readLine()) != null) {
					//System.out.println(lineTxt);
					f +=lineTxt;
				}
				read.close();
			} else {
				System.out.println("Can't find the file");
				return;
			}
		} catch (Exception e) {
			System.out.println("Read the file contains error!");
			e.printStackTrace();   
			return;
		}
		int s=0;//start
		int e=0;//end
		StringBuffer t = new StringBuffer(f);
		while((e=t.indexOf(";",s))!=-1)
		{
			s=filter(t,s);
			String p = t.substring(s, e+1);
			s = e + 1;
			sql = Interpreter.Inter(p);
			if(sql.equals("50"))//quit;
				BufferManager.writeBufferToFile();
			//System.out.println(sql);
			API.API_Moudle(sql);
		}
		sql = "99"; // stop now 
	}

	static int filter(StringBuffer SQL,int start)//
	{
		while(SQL.charAt(start) == ' ')
			start++;
		return start;
	}

	static String findCondition(String t,String table)
	{
		String tmp = null;
		StringBuffer k = new StringBuffer(t);
		int i = 0;
		int index;
		for(i = 0;i<t.length();i++)
			if(t.charAt(i)=='>'||t.charAt(i)=='<'||t.charAt(i)=='=')
				break;
		if(i==t.length())
		{
			sql = "99";
			System.out.println("Can't find attribution name!\r\n");
			tmp = "err";
			return tmp;
		}
		String att = t.substring(0, i);
		if(isValid(att))
		{
			att= att.trim();
			if(!CatalogManager.isAttribution(table, att))
			{
				sql = "99";
				System.out.println("\""+att+"\" is not a attribution of "+table+" !\r\n");
				tmp = "err";
				return tmp;
			}
			else
			{
				tmp = (att+"");
				//get the operator
				i=filter(k,i);
				index = i;
				for(;i<t.length();i++)
					if(t.charAt(i)!='>'&&t.charAt(i)!='<'&&t.charAt(i)!='=')
						break;
				if(i==t.length())//û���ҵ�op
				{
					sql = "99";
					System.out.println("Can't find operator!\r\n");
					tmp = "err";
					return tmp;
				}
				String op = t.substring(index, i);
				if(!(op.equals("<")||op.equals(">")||op.equals("=")||op.equals("<=")||op.equals(">=")||op.equals("<>")))
				{
					sql = "99";
					System.out.println("\""+op+"\" is not a valid operation!\r\n");
					tmp = "err";
					return tmp;
				}
				else
				{
					tmp += (op+"");
					i=filter(k,i);
					index = i;
					String num = t.substring(index);
					num= num.trim();
					if(!CatalogManager.Type(att, num, table))
					{
						sql = "99";
						System.out.println("Type of \""+num+"\" does not match the type of \""+att+"\"!\r\n");
						tmp = "err";
						return tmp;
					}
					else
					{
						if(num.charAt(0)=='\'')
							num=num.substring(1, num.length()-1);
						tmp += (num);
					}
				}
			}
		}
		else
		{
			sql = "99";
			System.out.println(att+" is not a valid attribution name!\r\n");
			tmp = "err";
			return tmp;
		}
		return tmp;
	}

	static void insert(StringBuffer SQL,int begin)
	{
		sql = "40 ";
		int start = filter(SQL,begin);
		int end = SQL.indexOf(" ",start);
		if(end == -1)
		{
			sql = "99";
			System.out.println("Incorrect use of \"insert\" commamd!Please check your input!!\r\n");
			return;
		}
		String word = SQL.substring(start, end);
		start = end + 1;
		if(word.equals("into"))
		{
			start = filter(SQL,start);
			end = SQL.indexOf("values",start);
			/*if(end == -1)
			{
				sql = "99";
				System.out.println("UNKNOW commamd!Please check your input!!\r\n");
				return;
			}*/
			
			if(end == -1)
			{
				sql = "99";
				System.out.println("Lacking key word:\"values\"!Please check your input!\r\n");
				return;
			}
			else
			{
				word = SQL.substring(start, end);
				start = end + 6;
				if(word.trim().isEmpty())
				{
					sql = "99";
					System.out.println("Can't find the table name!\r\n");
					return;
				}
				if(isValid(word))
				{
					word = word.trim();
					if(CatalogManager.getTable(word) != null)
					{
						sql += (word+" ");
						start = SQL.indexOf("(", start);
						if(start == -1)
						{
							sql = "99";
							System.out.println("Record should be include in \"(\" and\")\" !Please check your input!!\r\n");
							return;
						}
						start += 1;
						end = SQL.indexOf(")", start);
						if(end == -1)
						{
							sql = "99";
							System.out.println("Record should be include in \"(\" and\")\" !Please check your input!!\r\n");
							return;
						}
						if(start == -1 || end == -1)
						{
							sql = "99";
							System.out.println("Incorrect use of \"insert\" commamd!Please check your input!!\r\n");
							return;
						}
						else
						{
							int count = 1;
							while((end>start)&&(!SQL.substring(start, end).trim().isEmpty()))
							{
								start = filter(SQL,start);
								int p=SQL.indexOf(",", start);
								if(p == -1)
								{
									p = SQL.indexOf(")", start);
									if(p == -1)
									{
										sql = "99";
										System.out.println("commamd lack of \")\" !Please check your input!!\r\n");
										return;
									}
								}
								String t = SQL.substring(start, p);
								start = p + 1;
								if(CatalogManager.matchType(t,word,count))
								{
									if(t.charAt(0)=='\''&&t.charAt(t.length()-1)=='\'')
										t=t.substring(1, t.length()-1);
									if(count == 1)
									{
										sql += ("# "+t);
									}
									else
									{
										sql += (","+t);
									}
									count++;
								}
								else
								{
									sql = "99";
									System.out.println("\""+t+"\"" + " doesn't match the attribution!\r\n ");
									return;
								}
							}
							sql += " ; ";
						}
					}
					else
					{
						sql = "99";
						System.out.println("Table "+"\""+word+"\""+" has not been created!\r\n");
					}
				}
				else
				{
					sql = "99";
					System.out.println("\""+word+"\""+" is not a valid table name!\r\n");
					return;
				}
			}
		}
		else
		{
			sql = "99";
			System.out.println("Lacking key word:\"into\"!Please check your input!\r\n");
			return;
		}
	}

	public static String Inter(String input) throws Exception
	{
		String word = new String();
		char c;
		int start = 0;
		int end = 0;
		if(!input.isEmpty())
		{
			word = input;
			word = word.replace("\t", " ");
			word = word.replace("\r", " ");
			word = word.replace("\n", " ");
		}
		else
		{
			while((c = (char)System.in.read())!=';')
			{
				if(c != '\t'&&c != '\r'&& c != '\n')
					word += c;
				else 
					word += ' ';
			}
			word += c;
			//following can be added in windows
			//while((c = (char)System.in.read())!='\r')
			//{
			//	continue;
			//}
			
			c = (char)System.in.read();
		}
		StringBuffer SQL = new StringBuffer(word.toLowerCase());
		int e = SQL.lastIndexOf(";");
		if(!SQL.substring(e+1).trim().isEmpty())
		{
			sql = "99";
			System.out.println("Error:SQL commamd should be ended with';'!\r\n");
			return sql;
		}

		start = filter(SQL,start);
		end = SQL.indexOf(" ",start);
		if(end == -1)
		{
			end = SQL.indexOf(";",start);
			if(end == -1)
			{
				sql = "99";
				System.out.println("UNKONW command!Please check your input!\r\n");
				return sql;
			}
		}
		word = SQL.substring(start, end);
		start = end + 1;
		switch(word)
		{
			case "create":create_case(SQL,start);break;
			case "drop":drop_case(SQL,start);break;
			case "select":select(SQL,start);break;
			case "insert":insert(SQL,start);break;
			case "delete":delete(SQL,start);break;
			case "quit":sql="50";break;
			case "execfile":execfile(SQL,start);break;
			case "help":sql="70";break;
			default:sql="99";
				    System.out.println("\""+word+"\""+" is not an internal command!\r\n");
				    break;
		}
		
		return sql;		
	}

	static boolean isValid(String word)//
	{
		boolean flag = true;
		StringBuffer tmp = new StringBuffer(word);
		String p = tmp.toString();
		int start=0;
		start = filter(tmp,start);
		p += " ";
		int end = p.length() - 1;
		while(p.charAt(end) == ' ')
			end--;
		p = p.substring(start,end+1);
		for(int i=0;i<p.length();i++)
			if(p.charAt(i)<'a'||p.charAt(i)>'z')
				if(!(p.charAt(i)<='9'&&p.charAt(i)>='0')&&p.charAt(i)!='_'&&p.charAt(i)!='-')
					flag = false;
		
		return flag;
	}

	private static boolean isValidNum(String word) 
	{
		boolean flag = true;
		StringBuffer tmp = new StringBuffer(word);
		String p = tmp.toString();
		int start=0;
		start = filter(tmp,start);
		p += " ";
		int end = p.length() - 1;
		while(p.charAt(end) == ' ')
			end--;
		p = p.substring(start,end+1);
		for(int i=0;i<p.length();i++)
			if(p.charAt(i)<'0'||p.charAt(i)>'9')
				flag = false;
		
		return flag;
	}
	
	static void select(StringBuffer SQL,int begin)
	{
		sql = "30 ";
		int start = filter(SQL,begin);
		int tmp;
		String att = null;
		String tab = null;
		String where = null;
		tmp = SQL.indexOf("from", start);
		if(tmp != -1)
		{
			att = SQL.substring(start, tmp);
			if(att.trim().isEmpty())
			{
				System.out.println("the attribution is null!\r\n");
				sql="99";
				return;
			}
			start = tmp + 4;
		}
		else
		{
			sql = "99";
			System.out.println("Doesn't determin the attribution!\r\n");
			return;
		}
		
		start = filter(SQL,start);
		tmp = SQL.indexOf("where", start);
		if(tmp != -1)
		{
			tab = SQL.substring(start, tmp);
			start = tmp +5;
		}
		else
		{
			//no condition
			tmp = SQL.lastIndexOf(";");
			tab = SQL.substring(start, tmp);
			start = tmp;
		}
		start = filter(SQL,start);
		tmp = SQL.lastIndexOf(";");
		where = SQL.substring(start, tmp);
		
		if(tab.trim().isEmpty())
		{
			sql = "99";
			System.out.println("Doesn't determin the table!\r\n");
			return;
		}

		if(isValid(tab))
		{
			tab = tab.trim();
			if(CatalogManager.getTable(tab) != null)
			{
				sql += ("# "+tab+" ");
			}
			else
			{
				sql = "99";
				System.out.println("Table "+"\""+tab+"\""+" has not been created!\r\n");
				return;
			}
		}
		else
		{
			sql = "99";
			System.out.println("\""+tab+"\""+" is not a valid table name!\r\n");
			return;
		}

		if(att.charAt(0)=='*')
		{
			if(!att.substring(1).trim().isEmpty())
			{
				sql = "99";
				System.out.println("Format of selected attributions error!\r\n");
				return;
			}
			else
			{
				sql += "# * ";
			}
		}
		else
		{
			int b=0,e=0;
			String t = null;
			sql += "# ";
			while((e = att.indexOf(",",b)) != -1)
			{
				 t = att.substring(b, e);
				 b = e+1;
				 if(isValid(t))
				 {
					 t = t.trim();
					 if(CatalogManager.isAttribution(tab, t))
					 {
						 sql += (t + " ");
					 }
					 else
					 {
						 sql = "99";
						 System.out.println("\""+t+"\""+" is not a attribution of table "+"\""+tab+"\""+"\r\n");
						 return;
					 }
				 }
				 else
				 {
					 sql = "99";
					 System.out.println("\""+t+"\""+" is not a valid attribution name!\r\n");
					 return;
				 }
			}
			e = att.indexOf(" ", b);
			if(e==-1)
			{
				sql = "99";
				System.out.println("Incorrect use of \"select\" commamd!Please check your input!!\r\n");
				return;
			}
			t = att.substring(b, e);

			if(isValid(t))
			 {
				 t = t.trim();
				 if(CatalogManager.isAttribution(tab, t))
				 {
					 sql += (t + " ");
				 }
				 else
				 {
					 sql = "99";
					 System.out.println("\""+t+"\""+" is not a attribution of table "+"\""+tab+"\""+"\r\n");
					 return;
				 }
			 }
			 else
			 {
				 sql = "99";
				 System.out.println("\""+t+"\""+" is not a valid attribution name!\r\n");
				 return;
			 }
		}

		if(!where.trim().isEmpty())
		{
			int b=0,e;
			String t = null;
			while((e = where.indexOf("and", b))!=-1)
			{
				t = where.substring(b, e);
				b = e + 3;
				int length = t.length()-1;
				while(t.charAt(length) == ' ')
					length--;
				t = t.substring(0, length+1);
				t = findCondition(t,tab);
				sql += ("# "+t+" ");
			}
			t = where.substring(b);
			int length = t.length()-1;
			while(t.charAt(length) == ' ')
				length--;
			t = t.substring(0, length+1);
			t = t.trim();
			t = findCondition(t,tab);
			sql += ("# "+t+" ");
			
		}
		sql += "; ";
	}
}
