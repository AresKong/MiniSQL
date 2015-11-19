package Minisql_final;


import Minisql_final.Structure.*;
import java.io.UnsupportedEncodingException;


public  class BufferManager {
	
	static public int blockNum=0;	// the block number of the block,
	static private final int maxBlockNumber = 88;
	static private BufferBlock head = new BufferBlock();//head is a null block
	static private BufferBlock tail =  new BufferBlock();
	
	
	public static void writeBufferToFile(){
		BufferBlock block = head.next;
		while(block!=null){
			if(block.dirtyBit){
				try {
					DBfile.BlocktoFile(block.fileName, block.values, block.blockOffset, block.recordNum);
				} catch (Exception e) {
					e.printStackTrace();
				}
			}
			block.dirtyBit=false;
			block=block.next;
		}
	}
	
	//Get the block according to the filename and offset and lock it 
	static public BufferBlock readBlock( String FileName, int Offset,boolean lock){
		BufferBlock block =  readBlock(  FileName,  Offset);
		block.lock=true;
		return block;
	}
	
	//Get the block according to the filename and offset
	static public BufferBlock readBlock( String FileName, int Offset){
		BufferBlock tempblock = head.next;
		
		//search if it is in the buffer
		while(tempblock!=null){
			if(tempblock.fileName.equals(FileName) && (tempblock.blockOffset == Offset)){
				if(tempblock!=tail){
				tempblock.previous.next=tempblock.next;
				tempblock.next.previous=tempblock.previous;
				tempblock.previous=tail;
				tail.next=tempblock;
				tempblock.next=null;
				tail = tempblock;}
				return tempblock;
			}
			tempblock=tempblock.next;
		}
		
		//if it is not in the buffer, read from the file
		if(isFull()){
			writeBlockToFile(head.next);
		}
		else blockNum++;
		
		tempblock = new BufferBlock();
		try {
			tempblock=DBfile.FiletoBlock(FileName, Offset);
		} catch (Exception e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		tempblock.fileName = FileName;
		tempblock.blockOffset=Offset;
		tempblock.next=null;
		tempblock.dirtyBit=false;
		if(head.next==null){
			head.next=tempblock;
			tempblock.previous=head;
		}
		else {
			tail.next=tempblock;
			tempblock.previous=tail;
		}
		
		tail=tempblock;
		 
		return tempblock;
		
		
		
	}
	
	//Write the block back to the file 
	static public void writeBlockToFile(BufferBlock block) {
		if(block.lock){
			writeBlockToFile(block.next);
			return;
			}
		
		if(!block.dirtyBit);
		else 	
			try {
				DBfile.BlocktoFile( block.fileName,block.values, block.blockOffset,block.recordNum);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		if(block.next!=null)
			block.next.previous=block.previous;
		block.previous.next=block.next;
	}

	
	//return a block only lack values
	static public BufferBlock createBlock(String fileName,int blockOffset){
		if(isFull()){
			writeBlockToFile(head.next);
		}
		else blockNum++;
		
		BufferBlock block = new BufferBlock(fileName,blockOffset);
	
		if(head.next==null){
			head.next=block;
			block.previous=head;
		}
		else{
			tail.next=block;
			block.previous=tail;
		}
		tail = block;
		return block;
		
	}
	
	//Get the latest block
	static BufferBlock getInsertPosition(Table tableInfo,String filename){  
		BufferBlock block = readBlock(/*tableInfo.tableName*/filename, tableInfo.blockNum-1);
		if(block.recordNum<tableInfo.maxRecordsPerBlock)
			return block;
		else 
			return null;
		
	}
	
	static void deleteValues(int blockOffset,int offset,Table tableInfo) throws UnsupportedEncodingException{
		String filename = tableInfo.tableName+".table";
		int length = tableInfo.recordLength;
		BufferBlock b = readBlock(filename, blockOffset);
		b.delete(offset, length);
	}
	
	
	static public boolean isFull(){
		if(blockNum == maxBlockNumber)
			return true;
		else return false;
	}
	
	static public void dropTable(String filename){
		BufferBlock block = head.next;
		while(block!=null){
			if(block.fileName.equals(filename)){
				if(block.next!=null)
					block.next.previous=block.previous;
				block.previous.next=block.next;
			}
			block=block.next;
		}
	}
	
}


