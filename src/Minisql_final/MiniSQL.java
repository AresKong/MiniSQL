package Minisql_final;

public class MiniSQL {
	
	public static void main(String[] args) {
		// TODO Auto-generated method stub
		CatalogManager.ReadCatalog();
		System.out.println("Welcome to MiniSQL!");
		System.out.println();
		
 		while(true)
		{
			System.out.print("minisql>>");
			String SQL = new String();
			try{
				SQL = Interpreter.Inter(SQL);
				//System.out.println("SQL:"+SQL);
				API.API_Moudle(SQL);
				if(SQL.equals("50"))
					break;
				
			}catch(Exception e)
			{
				e.printStackTrace();
			}		
		}
 		CatalogManager.UpdateCatalog();
		BufferManager.writeBufferToFile();
	}

}
