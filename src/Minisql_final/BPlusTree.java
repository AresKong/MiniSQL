package Minisql_final;

import Minisql_final.Structure.*;
import java.io.*;

class BPlusTree{
	
    private static final int POINTERLENGTH = 4; 
    
    private int MIN_CHILDREN_FOR_INTERNAL; 
        
	private int MAX_CHILDREN_FOR_INTERNAL;
	
	private int MIN_FOR_LEAF;  
	
	private int MAX_FOR_LEAF; 
  	
	public String filename;
	
	public BufferBlock myRootBlock;
	
	public Index myIndexInfo;
	
	BPlusTree(Index indexInfo/*,BufferManager buffer*/){
		
		try{	
			 filename=indexInfo.indexName+".index";
			 PrintWriter out = new PrintWriter( new BufferedWriter(new FileWriter(new File(filename))));
			 //out.write("#@0\r\n#&0\r\n");
			 out.close();
		}catch(Exception e){
			 System.err.println("create index failed !");
	    }
		
		int columnLength=indexInfo.columnLength;
		
		//8: 4 for block offset and 4 for inblock offset
		MAX_FOR_LEAF=(int)Math.floor((4096.0-1/*is leaf*/-4/*indexNum*/-POINTERLENGTH/*point to father*/-POINTERLENGTH/*point to next sibling*/)/(8+columnLength));
		MIN_FOR_LEAF=(int)Math.ceil(1.0 * MAX_FOR_LEAF/ 2);	
		MAX_CHILDREN_FOR_INTERNAL=MAX_FOR_LEAF; 
		MIN_CHILDREN_FOR_INTERNAL=(int)Math.ceil(1.0 *(MAX_CHILDREN_FOR_INTERNAL)/ 2);
		
		CatalogManager.setIndexRoot(indexInfo.indexName, 0);
		myIndexInfo=indexInfo;
		myIndexInfo.blockNum++;

		new LeafNode(myRootBlock=BufferManager.createBlock(filename,0)); //create first leafnode
		
	}

	abstract class Node {
		BufferBlock block;
		
		Node createNode(BufferBlock blk){
			block=blk;
			return this;
		}

		abstract BufferBlock delete(byte[] deleteKey);
		abstract BufferBlock insert(byte[] inserKey,int blockOffset, int offset);
		abstract offsetInfo searchKey(byte[] Key);
    }  
	
	BPlusTree(Index indexInfo,BufferManager buffer,int rootBlockNum){
		int columnLength=indexInfo.columnLength; 
		MAX_FOR_LEAF=(int)Math.floor((4096.0-1/*is leaf*/-4/*indexNum*/-POINTERLENGTH/*point to father*/-POINTERLENGTH/*point to next sibling*/)/(8+columnLength));
		MIN_FOR_LEAF=(int)Math.ceil(1.0 * MAX_FOR_LEAF/ 2);	
		MAX_CHILDREN_FOR_INTERNAL=MAX_FOR_LEAF; 
		MIN_CHILDREN_FOR_INTERNAL=(int)Math.ceil(1.0 *(MAX_CHILDREN_FOR_INTERNAL)/ 2);
		
		myIndexInfo=indexInfo;	
		filename = myIndexInfo.indexName+".index";
		new LeafNode(myRootBlock=BufferManager.readBlock(filename,rootBlockNum),true); //readblock

	}
	class InternalNode extends Node{
		
		InternalNode(BufferBlock blk){		
			block=blk; 
	    	
	    	block.values[0]='I';  
			block.setInt(1, 4, 0);
			int i=5;
	    	for(;i<9;i++)  block.values[i]='$'; // no father pointer
		}
		
		InternalNode(BufferBlock blk,boolean t){		
			block=blk; 
		}
		
		// branchKey: the key to be insert
		BufferBlock branchInsert(byte[] branchKey,Node leftChild,Node rightChild){
			int keyNum = block.getInt(1, 4);//indexNum
			
			if(keyNum==0){ 
				keyNum++;
				block.setInt(1, 4, keyNum);
				block.setBytes(9+POINTERLENGTH, branchKey); 
				block.setInt(9, POINTERLENGTH, leftChild.block.blockOffset);
				block.setInt(9+POINTERLENGTH+branchKey.length, POINTERLENGTH, rightChild.block.blockOffset);
				
				return this.block; 
			}
			
			if(++keyNum>MAX_CHILDREN_FOR_INTERNAL){  
				boolean half=false; 
				int newBlockOffset=myIndexInfo.blockNum;
				myIndexInfo.blockNum++;
				BufferBlock newBlock=BufferManager.createBlock(filename, newBlockOffset);
				InternalNode newNode=new InternalNode(newBlock);
				
				// Max+1 = Min + (Max+1-Min)
				block.setInt(1, 4, MIN_CHILDREN_FOR_INTERNAL);
				newBlock.setInt(1, 4, MAX_CHILDREN_FOR_INTERNAL+1-MIN_CHILDREN_FOR_INTERNAL);
				
				for(int i=0;i<MIN_CHILDREN_FOR_INTERNAL;i++){ 
					
					int pos=9+POINTERLENGTH+i*(myIndexInfo.columnLength+POINTERLENGTH);
					if(compareTo(branchKey,block.getBytes(pos,myIndexInfo.columnLength))< 0){	//if the target is in the Min
								
						System.arraycopy(block.values,  //copy the following record to the newblock
								9+(MIN_CHILDREN_FOR_INTERNAL)*(myIndexInfo.columnLength+POINTERLENGTH), 
								newBlock.values, 
								9, 
								POINTERLENGTH+(MAX_CHILDREN_FOR_INTERNAL-MIN_CHILDREN_FOR_INTERNAL)*(myIndexInfo.columnLength+POINTERLENGTH));	
						System.arraycopy(block.values,  //leave space for the target index
								9+POINTERLENGTH+i*(myIndexInfo.columnLength+POINTERLENGTH), 
								block.values, 
								9+POINTERLENGTH+(i+1)*(myIndexInfo.columnLength+POINTERLENGTH),
								
								(MIN_CHILDREN_FOR_INTERNAL-1-i)*(myIndexInfo.columnLength+POINTERLENGTH)+myIndexInfo.columnLength);	

						block.setInternalKey(9+POINTERLENGTH+i*(myIndexInfo.columnLength+POINTERLENGTH),branchKey,rightChild.block.blockOffset);				
											
						half=true;
						break;
					}
				}				
				if(!half){ // if the target is in the Max+1-Min
					System.arraycopy(block.values,  
							9+(MIN_CHILDREN_FOR_INTERNAL+1)*(myIndexInfo.columnLength+POINTERLENGTH), 
							newBlock.values, 
							9, 
							POINTERLENGTH+(MAX_CHILDREN_FOR_INTERNAL-MIN_CHILDREN_FOR_INTERNAL-1)*(myIndexInfo.columnLength+POINTERLENGTH));
					for(int i=0;i<MAX_CHILDREN_FOR_INTERNAL-MIN_CHILDREN_FOR_INTERNAL-1;i++){
						int pos=9+POINTERLENGTH+i*(myIndexInfo.columnLength+POINTERLENGTH);
						
						if(compareTo(branchKey,newBlock.getBytes(pos,myIndexInfo.columnLength)) < 0){
							System.arraycopy(newBlock.values,   // leave space for the target index
									9+POINTERLENGTH+i*(myIndexInfo.columnLength+POINTERLENGTH),
									newBlock.values, 
									9+POINTERLENGTH+(i+1)*(myIndexInfo.columnLength+POINTERLENGTH),
									(MAX_CHILDREN_FOR_INTERNAL-MIN_CHILDREN_FOR_INTERNAL-1-i)*(myIndexInfo.columnLength+POINTERLENGTH));								
							
							newBlock.setInternalKey(9+POINTERLENGTH+i*(myIndexInfo.columnLength+POINTERLENGTH),branchKey,rightChild.block.blockOffset);				
							break;							
						}	
					}
				}
				
				byte[] spiltKey=block.getBytes(9+POINTERLENGTH+(MIN_CHILDREN_FOR_INTERNAL)*(myIndexInfo.columnLength+POINTERLENGTH),
						myIndexInfo.columnLength);
				
				//set the father for the new block
				for(int j=0;j<=newBlock.getInt(1, 4);j++){
					int childBlockNum=newBlock.getInt(9+j*(myIndexInfo.columnLength+POINTERLENGTH),POINTERLENGTH);
					BufferManager.readBlock(filename, childBlockNum).setInt(5, POINTERLENGTH, newBlockOffset);					
				}	
				
				int parentBlockNum;
				BufferBlock ParentBlock;
				InternalNode ParentNode;
				if(block.values[5]=='$'){  //no father, create it 
					parentBlockNum=myIndexInfo.blockNum;
					ParentBlock=BufferManager.createBlock(filename, parentBlockNum);
					myIndexInfo.blockNum++;
					
					block.setInt(5, POINTERLENGTH, parentBlockNum);
					newBlock.setInt(5, POINTERLENGTH, parentBlockNum );
					
					ParentNode=new InternalNode(ParentBlock);
				}
				else{
					parentBlockNum=block.getInt(5,POINTERLENGTH);				
					newBlock.setInt(5, POINTERLENGTH, parentBlockNum); 
					ParentBlock=BufferManager.readBlock(filename, parentBlockNum);	
					ParentNode=new InternalNode(ParentBlock,true);
				}
				
				//recursively branchInsert the father node 
				return  ParentNode.branchInsert(spiltKey, this, newNode);//((InternalNode)createNode(ParentBlock)).branchInsert(spiltKey, this, newNode);
			}
			
			else{  //no need to branch
				int i;
				for(i=0;i<keyNum-1;i++){
					int pos=9+POINTERLENGTH+i*(myIndexInfo.columnLength+POINTERLENGTH);
					if(compareTo(branchKey,block.getBytes(pos,myIndexInfo.columnLength)) < 0){ //find the insert position
						System.arraycopy(block.values,
										9+POINTERLENGTH+i*(myIndexInfo.columnLength+POINTERLENGTH), 
										block.values, 
										9+POINTERLENGTH+(i+1)*(myIndexInfo.columnLength+POINTERLENGTH), 
										(keyNum-1-i)*(myIndexInfo.columnLength+POINTERLENGTH));
						
						block.setInternalKey(9+POINTERLENGTH+i*(myIndexInfo.columnLength+POINTERLENGTH),branchKey,rightChild.block.blockOffset);									
						block.setInt(1, 4, keyNum);
						
						return null;
					}					
				}
				if(i==keyNum-1){				
						block.setInternalKey(9+POINTERLENGTH+i*(myIndexInfo.columnLength+POINTERLENGTH),branchKey,rightChild.block.blockOffset);									
						block.setInt(1, 4, keyNum);
						
						return null;							
				}
			}
						
			return null;
		}
		
		//delete a child block on internal block
		BufferBlock	delete(BufferBlock blk){
			int keyNum = block.getInt(1, 4);
			
			for(int i=0;i<=keyNum;i++){
				int pos=9+i*(myIndexInfo.columnLength+POINTERLENGTH);
				int ptr=block.getInt(pos, POINTERLENGTH);
				if(ptr==blk.blockOffset){ //if found 
					System.arraycopy(block.values, 
							9+POINTERLENGTH+(i-1)*(myIndexInfo.columnLength+POINTERLENGTH), 
							block.values, 
							9+POINTERLENGTH+i*(myIndexInfo.columnLength+POINTERLENGTH), 
							(keyNum-i)*(myIndexInfo.columnLength+POINTERLENGTH));
					keyNum--;
					block.setInt(1, 4, keyNum);
			
					if(keyNum >=MIN_CHILDREN_FOR_INTERNAL) return null; 
			
					if(block.values[5]=='$'){  
						
						if(keyNum==0){	 //delete this block
							myIndexInfo.blockNum--;
							return BufferManager.readBlock(filename, block.getInt(9, POINTERLENGTH));
						}
							
						return null;
					}
					
					int parentBlockNum=block.getInt(5, POINTERLENGTH);
					BufferBlock parentBlock=BufferManager.readBlock(filename, parentBlockNum);
					int parentKeyNum=parentBlock.getInt(1, 4);
					
					int sibling;
					BufferBlock siblingBlock;
					int j=0;
					for(;j<parentKeyNum;j++){
						int ppos=9+j*(myIndexInfo.columnLength+POINTERLENGTH);
						if(block.blockOffset==parentBlock.getInt(ppos, POINTERLENGTH)){ 
							sibling=parentBlock.getInt(ppos+POINTERLENGTH+myIndexInfo.columnLength, POINTERLENGTH);
							siblingBlock=BufferManager.readBlock(filename, sibling);
								
							byte[] unionKey=parentBlock.getBytes(ppos+POINTERLENGTH, myIndexInfo.columnLength);
							
							if((siblingBlock.getInt(1, 4)+keyNum)<=MAX_CHILDREN_FOR_INTERNAL){				
								return this.union(unionKey,siblingBlock);
							}
							
							if(siblingBlock.getInt(1, 4)==MIN_CHILDREN_FOR_INTERNAL) return null;
							
							(new InternalNode(parentBlock,true)).exchange(rearrangeAfter(siblingBlock,unionKey),block.blockOffset);//blockOffset��bufferManager�����ƺ�
							return null;
					
						}				
					}
					
					sibling=parentBlock.getInt(9+(parentKeyNum-1)*(myIndexInfo.columnLength+POINTERLENGTH), POINTERLENGTH);
					siblingBlock=BufferManager.readBlock(filename, sibling);		
								
					byte[] unionKey=parentBlock.getBytes(9+(parentKeyNum-1)*(myIndexInfo.columnLength+POINTERLENGTH)+POINTERLENGTH, myIndexInfo.columnLength);
					
					if((siblingBlock.getInt(1, 4)+keyNum)<=MAX_CHILDREN_FOR_INTERNAL){		
						return (new InternalNode(siblingBlock,true)).union(unionKey,block);
					}
						
					if(siblingBlock.getInt(1, 4)==MIN_CHILDREN_FOR_INTERNAL) return null;
					
					(new InternalNode(parentBlock,true)).exchange(rearrangeBefore(siblingBlock,unionKey),sibling);
					return null;
				}
	
			}		
			return null;
		}
		
		//delete on the internal node
		BufferBlock delete(byte[] deleteKey){
			int keyNum=block.getInt(1, 4);
			int i=0;
			for(;i<keyNum;i++){
				int pos=9+POINTERLENGTH+i*(myIndexInfo.columnLength+POINTERLENGTH);
				if(compareTo(deleteKey,block.getBytes(pos,myIndexInfo.columnLength)) < 0) break;
			}
			int nextBlockNum=block.getInt(9+i*(myIndexInfo.columnLength+POINTERLENGTH), POINTERLENGTH);
			BufferBlock nextBlock=BufferManager.readBlock(filename, nextBlockNum);
			Node nextNode;
			if(nextBlock.values[0]=='I') nextNode=new InternalNode(nextBlock,true); 
			else nextNode=new LeafNode(nextBlock,true);
			
			return nextNode.delete(deleteKey); //recursively delete 
		}

		//change the index after posBlockNum
		public void exchange(byte[] changeKey,int posBlockNum){
			int keyNum = block.getInt(1, 4);
			
			int i=0;
			for(;i<keyNum;i++){
				int pos=9+i*(myIndexInfo.columnLength+POINTERLENGTH);
				int blockNum=block.getInt(pos,POINTERLENGTH);
				if(blockNum==posBlockNum) break;
			}
			
			if(i<keyNum) block.setBytes(9+i*(myIndexInfo.columnLength+POINTERLENGTH)+POINTERLENGTH, changeKey);
		}
	
		//insert on InternalNode 
		BufferBlock insert(byte[] insertKey,int blockOffset, int offset){
			int keyNum=block.getInt(1, 4); //indexNum
					
			int i=0;
			for(;i<keyNum;i++){
				int pos=9+POINTERLENGTH+i*(myIndexInfo.columnLength+POINTERLENGTH);
				if(compareTo(insertKey, block.getBytes(pos,myIndexInfo.columnLength)) < 0) break;
			}
			
			int nextBlockNum=block.getInt(9+i*(myIndexInfo.columnLength+POINTERLENGTH), POINTERLENGTH);
			BufferBlock nextBlock=BufferManager.readBlock(filename, nextBlockNum); //read child's block
			
			Node nextNode;
			if(nextBlock.values[0]=='I') nextNode=new InternalNode(nextBlock,true);  
			else nextNode=new LeafNode(nextBlock,true);
			
			return nextNode.insert(insertKey, blockOffset, offset); 
		}
		
		//rearrange the sibling blocks
		byte[] rearrangeAfter(BufferBlock siblingBlock,byte[] InternalKey){ 
			int siblingKeyNum=siblingBlock.getInt(1, 4);
			int keyNum = block.getInt(1, 4);
			
			int blockOffset=siblingBlock.getInt(9,POINTERLENGTH);
			block.setInternalKey(9+POINTERLENGTH+keyNum*(myIndexInfo.columnLength+POINTERLENGTH), InternalKey, blockOffset);
			keyNum++;
			block.setInt(1, 4, keyNum);
			
			
			siblingKeyNum--;
			siblingBlock.setInt(1, 4, siblingKeyNum);
			byte[] changeKey=siblingBlock.getBytes(9+POINTERLENGTH, myIndexInfo.columnLength);
			System.arraycopy(siblingBlock.values, 9+POINTERLENGTH+myIndexInfo.columnLength, siblingBlock.values, 9, POINTERLENGTH+siblingKeyNum*(POINTERLENGTH+myIndexInfo.columnLength));
					
			return changeKey;
			
		}

		byte[] rearrangeBefore(BufferBlock siblingBlock,byte[] internalKey){ //siblingBlock is before this
			int siblingKeyNum=siblingBlock.getInt(1, 4);
			int keyNum = block.getInt(1, 4);
			
			siblingKeyNum--;
			siblingBlock.setInt(1, 4, siblingKeyNum);
			
			byte[] changeKey=siblingBlock.getBytes(9+POINTERLENGTH+siblingKeyNum*(POINTERLENGTH+myIndexInfo.columnLength), myIndexInfo.columnLength);		
			int blockOffset=siblingBlock.getInt(9+(siblingKeyNum+1)*(POINTERLENGTH+myIndexInfo.columnLength), POINTERLENGTH);
			
			//give the last index of siblingBlock to this
			System.arraycopy(block.values, 9, block.values, 9+POINTERLENGTH+myIndexInfo.columnLength, POINTERLENGTH+keyNum*(POINTERLENGTH+myIndexInfo.columnLength));
			block.setInt(9, POINTERLENGTH, blockOffset); 
			block.setBytes(9+POINTERLENGTH, internalKey); 
			keyNum++;
			block.setInt(1, 4, keyNum);
					
			return changeKey;
		}

		//search on the internal node 
		offsetInfo searchKey(byte[] key){
			int keyNum=block.getInt(1, 4);
			int i=0;
			for(;i<keyNum;i++){
				int pos=9+POINTERLENGTH+i*(myIndexInfo.columnLength+POINTERLENGTH);
				
				if(compareTo(key,block.getBytes(pos,myIndexInfo.columnLength)) < 0) break;
			}
			int nextBlockNum=block.getInt(9+i*(myIndexInfo.columnLength+POINTERLENGTH), POINTERLENGTH);
			BufferBlock nextBlock=BufferManager.readBlock(filename, nextBlockNum);
			//packing according to type
			Node nextNode;
			if(nextBlock.values[0]=='I') nextNode=new InternalNode(nextBlock,true); 
			else nextNode=new LeafNode(nextBlock,true);
			
			return nextNode.searchKey(key); //recursively search 
		}
		
		//union the node 
		BufferBlock union(byte[] unionKey,BufferBlock afterBlock){
			int keyNum = block.getInt(1, 4);
			int afterkeyNum= afterBlock.getInt(1, 4);
			
			System.arraycopy(afterBlock.values,
					9,
					block.values,
					9+(keyNum+1)*(myIndexInfo.columnLength+POINTERLENGTH),
					POINTERLENGTH+afterkeyNum*(myIndexInfo.columnLength+POINTERLENGTH));
			
			//insert unionKey
			block.setBytes(9+keyNum*(myIndexInfo.columnLength+POINTERLENGTH)+POINTERLENGTH, unionKey);
			
			//calculate the new index number
			keyNum=keyNum+afterkeyNum+1;		
			block.setInt(1, 4, keyNum);
			
			//find the father block 
			int parentBlockNum=block.getInt(5, POINTERLENGTH);
			BufferBlock parentBlock=BufferManager.readBlock(filename, parentBlockNum); 
			
			myIndexInfo.blockNum--;
			
			//delete after block in the father block 
			return (new InternalNode(parentBlock,true)).delete(afterBlock);
			
		}
				
	}
	class LeafNode extends Node{
				
		LeafNode(BufferBlock blk){
			block=blk;
			
	    	block.values[0]='L'; 
	    	int i=5;
			block.setInt(1, 4, 0);
	    	for(;i<9;i++)
	    		block.values[i]='$'; 
	    	for(;i<13;i++)
	    		block.values[i]='&';  
		}
		
		LeafNode(BufferBlock blk,boolean t){
			block=blk;	
		}

		BufferBlock delete(byte[] deleteKey){
			
			int keyNum = block.getInt(1, 4);
			
			for(int i=0;i<keyNum;i++){
				int pos=17+i*(myIndexInfo.columnLength+8);
				
				if(compareTo(deleteKey,block.getBytes(pos,myIndexInfo.columnLength))<0){ 
					System.out.println("delete failure");
					return null;
				}
				
				if(compareTo(deleteKey,block.getBytes(pos,myIndexInfo.columnLength)) == 0){ 
					
					System.arraycopy(block.values, 
									9+(i+1)*(myIndexInfo.columnLength+8), 
									block.values, 
									9+i*(myIndexInfo.columnLength+8), 
									POINTERLENGTH+(keyNum-1-i)*(myIndexInfo.columnLength+8));
					keyNum--;
					block.setInt(1, 4, keyNum);
					
					if(keyNum >=MIN_FOR_LEAF) return null; 
					
					if(block.values[5]=='$') return null;  
					
					boolean lastFlag=false;
					if(block.values[9+keyNum*(myIndexInfo.columnLength+8)]=='&') lastFlag=true; 
					
					int sibling=block.getInt(9+keyNum*(myIndexInfo.columnLength+8), POINTERLENGTH); 
					BufferBlock siblingBlock=BufferManager.readBlock(filename, sibling);
					int parentBlockNum=block.getInt(5, POINTERLENGTH);
					
					if(lastFlag || siblingBlock==null || siblingBlock.getInt(5, POINTERLENGTH)!=parentBlockNum){
						BufferBlock parentBlock=BufferManager.readBlock(filename, parentBlockNum);
						int j=0;
						int parentKeyNum=parentBlock.getInt(1, 4);
						for(;j<parentKeyNum;j++){
							int ppos=9+POINTERLENGTH+j*(myIndexInfo.columnLength+POINTERLENGTH);
							if(compareTo(deleteKey,parentBlock.getBytes(ppos, myIndexInfo.columnLength))<0){
								sibling=parentBlock.getInt(ppos-2*POINTERLENGTH-myIndexInfo.columnLength, POINTERLENGTH);
								siblingBlock=BufferManager.readBlock(filename, sibling);
								break;
							}
						}
						
						if((siblingBlock.getInt(1, 4)+keyNum)<=MAX_FOR_LEAF){
							return (new LeafNode(siblingBlock,true)).union(block);
						}
									
						if(siblingBlock.getInt(1, 4)==MIN_FOR_LEAF) return null;
							
						(new InternalNode(parentBlock,true)).exchange(rearrangeBefore(siblingBlock),sibling);
						return null;
					}
			
					//BufferBlock siblingBlock;
					if((siblingBlock.getInt(1, 4)+keyNum)<=MAX_FOR_LEAF){
						return this.union(siblingBlock);
					}
					
					if(siblingBlock.getInt(1, 4)==MIN_FOR_LEAF) return null;
					
					BufferBlock parentBlock=BufferManager.readBlock(filename, parentBlockNum);
					(new InternalNode(parentBlock,true)).exchange(rearrangeAfter(siblingBlock),block.blockOffset);//blockOffset��bufferManager�����ƺ�
					return null;
				}
			}
			
			return null;
		}
		
		BufferBlock insert(byte[] insertKey,int blockOffset, int offset){
			int keyNum = block.getInt(1, 4);
			
			if(++keyNum>MAX_FOR_LEAF){  
				boolean half=false;
				BufferBlock newBlock=BufferManager.createBlock(filename, myIndexInfo.blockNum);
				//CatalogManager.addIndexBlockNum(myIndexInfo.indexName);
				myIndexInfo.blockNum++;
				LeafNode newNode=new LeafNode(newBlock);
				
				for(int i=0;i<MIN_FOR_LEAF-1;i++){ 
					int pos=17+i*(myIndexInfo.columnLength+8);
					if(compareTo( insertKey,block.getBytes(pos,myIndexInfo.columnLength))< 0){					
						System.arraycopy(block.values,  
								9+(MIN_FOR_LEAF-1)*(myIndexInfo.columnLength+8), 
								newBlock.values, 
								9, 
								POINTERLENGTH+(MAX_FOR_LEAF-MIN_FOR_LEAF+1)*(myIndexInfo.columnLength+8));	
						System.arraycopy(block.values,  
								9+i*(myIndexInfo.columnLength+8), 
								block.values, 
								9+(i+1)*(myIndexInfo.columnLength+8),
								POINTERLENGTH+(MIN_FOR_LEAF-1-i)*(myIndexInfo.columnLength+8));	
						
						block.setKeyValues(9+i*(myIndexInfo.columnLength+8),insertKey,blockOffset,offset);				
											
						half=true;
						break;
					}
				}				
				if(!half){
					System.arraycopy(block.values,  
							9+(MIN_FOR_LEAF)*(myIndexInfo.columnLength+8), 
							newBlock.values, 
							9, 
							POINTERLENGTH+(MAX_FOR_LEAF-MIN_FOR_LEAF)*(myIndexInfo.columnLength+8));
					int i=0;
					for(;i<MAX_FOR_LEAF-MIN_FOR_LEAF;i++){
						int pos=17+i*(myIndexInfo.columnLength+8);
						if(compareTo(insertKey,newBlock.getBytes(pos,myIndexInfo.columnLength)) < 0){
							System.arraycopy(newBlock.values, 
									9+i*(myIndexInfo.columnLength+8), 
									newBlock.values, 
									9+(i+1)*(myIndexInfo.columnLength+8), 
									POINTERLENGTH+(MAX_FOR_LEAF-MIN_FOR_LEAF-i)*(myIndexInfo.columnLength+8));								
							
							newBlock.setKeyValues(9+i*(myIndexInfo.columnLength+8),insertKey,blockOffset,offset);
							break;
						}	
					}
					if(i==MAX_FOR_LEAF-MIN_FOR_LEAF){
						System.arraycopy(newBlock.values,  
								9+i*(myIndexInfo.columnLength+8), 
								newBlock.values, 
								9+(i+1)*(myIndexInfo.columnLength+8), 
								POINTERLENGTH+(MAX_FOR_LEAF-MIN_FOR_LEAF-i)*(myIndexInfo.columnLength+8));								
						
						newBlock.setKeyValues(9+i*(myIndexInfo.columnLength+8),insertKey,blockOffset,offset);
					}
				}
				
				block.setInt(1,4,MIN_FOR_LEAF);
			    newBlock.setInt(1,4,MAX_FOR_LEAF+1-MIN_FOR_LEAF);
			    
			    block.setInt(9+MIN_FOR_LEAF*(myIndexInfo.columnLength+8), POINTERLENGTH, newBlock.blockOffset);
				
			    int parentBlockNum;
			    BufferBlock ParentBlock;
			    InternalNode ParentNode;
				if(block.values[5]=='$'){  
					parentBlockNum=myIndexInfo.blockNum;
					ParentBlock=BufferManager.createBlock(filename, parentBlockNum);
				
					//CatalogManager.addIndexBlockNum(myIndexInfo.indexName);
					myIndexInfo.blockNum++;
		
					block.setInt(5, POINTERLENGTH, parentBlockNum);
					newBlock.setInt(5, POINTERLENGTH, parentBlockNum );
					ParentNode=new InternalNode(ParentBlock);
				}
				else{
					parentBlockNum=block.getInt(5,POINTERLENGTH);				
					newBlock.setInt(5, POINTERLENGTH, parentBlockNum); 

					ParentBlock=BufferManager.readBlock(filename, parentBlockNum);
					ParentNode=new InternalNode(ParentBlock,true);
				}
			
				byte[] branchKey=newBlock.getBytes(17, myIndexInfo.columnLength);
				
				return  ParentNode.branchInsert(branchKey, this, newNode);
			}
			
			else{  
				if(keyNum-1==0){
					System.arraycopy(block.values,
							9, 
							block.values, 
							9+(myIndexInfo.columnLength+8), 
							POINTERLENGTH);
			
					block.setKeyValues(9,insertKey,blockOffset,offset);						
					block.setInt(1, 4, keyNum);
			
					return null;
				}
				int i; 
				for(i=0;i<keyNum;i++){
					int pos=17+i*(myIndexInfo.columnLength+8);
					
					if(compareTo(insertKey,block.getBytes(pos,myIndexInfo.columnLength))==0){ 
						block.setKeyValues(9+i*(myIndexInfo.columnLength+8),insertKey,blockOffset,offset);
						return null;
					}
					
					if(compareTo(insertKey,block.getBytes(pos,myIndexInfo.columnLength)) < 0){ 
						System.arraycopy(block.values,
										9+i*(myIndexInfo.columnLength+8), 
										block.values, 
										9+(i+1)*(myIndexInfo.columnLength+8), 
										POINTERLENGTH+(keyNum-1-i)*(myIndexInfo.columnLength+8));
						
						block.setKeyValues(9+i*(myIndexInfo.columnLength+8),insertKey,blockOffset,offset);						
						block.setInt(1, 4, keyNum);
						
						return null;
					}					
				}
				if(i==keyNum){
					System.arraycopy(block.values,
							9+(i-1)*(myIndexInfo.columnLength+8), 
							block.values, 
							9+i*(myIndexInfo.columnLength+8), 
							POINTERLENGTH);
			
					block.setKeyValues(9+(i-1)*(myIndexInfo.columnLength+8),insertKey,blockOffset,offset);						
					block.setInt(1, 4, keyNum);
			
					return null;
				}
			}
		    return null;		
		}

		byte[] rearrangeAfter(BufferBlock siblingBlock){
			int siblingKeyNum=siblingBlock.getInt(1, 4);
			int keyNum = block.getInt(1, 4);
			
			int blockOffset=siblingBlock.getInt(9, 4);
			int offset=siblingBlock.getInt(13, 4);
			byte[] Key=siblingBlock.getBytes(17, myIndexInfo.columnLength);
			
			siblingKeyNum--;
			siblingBlock.setInt(1, 4, siblingKeyNum);
			System.arraycopy(siblingBlock.values, 9+8+myIndexInfo.columnLength, siblingBlock.values, 9, POINTERLENGTH+siblingKeyNum*(8+myIndexInfo.columnLength));
			
			byte[] changeKey=siblingBlock.getBytes(17, myIndexInfo.columnLength);
			
			block.setKeyValues(9+keyNum*(myIndexInfo.columnLength+8), Key, blockOffset, offset);
			keyNum++;
			block.setInt(1, 4, keyNum);
			block.setInt(9+keyNum*(myIndexInfo.columnLength+8), POINTERLENGTH, siblingBlock.blockOffset);
			
			return changeKey;
			
		}
		
		byte[] rearrangeBefore(BufferBlock siblingBlock){ 
			int siblingKeyNum=siblingBlock.getInt(1, 4);
			int keyNum = block.getInt(1, 4);
			
			siblingKeyNum--;
			siblingBlock.setInt(1, 4, siblingKeyNum);

			int blockOffset=siblingBlock.getInt(9+siblingKeyNum*(myIndexInfo.columnLength+8), 4);
			int offset=siblingBlock.getInt(13+siblingKeyNum*(myIndexInfo.columnLength+8), 4);
			byte[] Key=siblingBlock.getBytes(17+siblingKeyNum*(myIndexInfo.columnLength+8), myIndexInfo.columnLength);
			
			siblingBlock.setInt(9+siblingKeyNum*(myIndexInfo.columnLength+8), POINTERLENGTH, block.blockOffset);
			
			System.arraycopy(block.values, 9, block.values, 9+8+myIndexInfo.columnLength, POINTERLENGTH+keyNum*(8+myIndexInfo.columnLength));
			block.setKeyValues(9, Key, blockOffset, offset);
			keyNum++;
			block.setInt(1, 4, keyNum);

			byte[] changeKey=block.getBytes(17, myIndexInfo.columnLength);
			
			return changeKey;
		}
		
		offsetInfo searchKey(byte[] originalkey){
			int keyNum=block.getInt(1, 4); 
			if(keyNum==0) return null; 
		
			byte[] key=new byte[myIndexInfo.columnLength];
			
			int i=0;
			for(;i<originalkey.length;i++){
				key[i]=originalkey[i];
			}
			
		    for(;i<myIndexInfo.columnLength;i++){
				key[i]='&';
			}
			int start=0;
			int end=keyNum-1;
			int middle=0;

			while (start <= end) {  

				middle = (start + end) / 2;
								
                byte[] middleKey = block.getBytes(17+middle*(myIndexInfo.columnLength+8), myIndexInfo.columnLength);  
                if (compareTo(key,middleKey) == 0){  
                    break;  
                }  
                  
                if (compareTo(key,middleKey) < 0) {  
                    end = middle-1;  
                } else {  
                    start = middle+1;  
                }  
                
            }  
              			
			int pos=9+middle*(myIndexInfo.columnLength+8);
			byte[] middleKey = block.getBytes(8+pos, myIndexInfo.columnLength); 
			
            offsetInfo off=new offsetInfo();
          
            off.offsetInfile=block.getInt(pos, 4);
            off.offsetInBlock=block.getInt(pos+4, 4);
					
            return compareTo(middleKey,key) == 0 ? off : null;  
		}

		BufferBlock union(BufferBlock afterBlock){
			int keyNum = block.getInt(1, 4);
			int afterkeyNum= afterBlock.getInt(1, 4);
			
			System.arraycopy(afterBlock.values,9,block.values,9+keyNum*(myIndexInfo.columnLength+8),POINTERLENGTH+afterkeyNum*(myIndexInfo.columnLength+8));
			
			keyNum+=afterkeyNum;		
			block.setInt(1, 4, keyNum);
						
			//afterBlock.isvalid=false;
			myIndexInfo.blockNum--;
			
			int parentBlockNum=block.getInt(5, POINTERLENGTH);
			BufferBlock parentBlock=BufferManager.readBlock(filename, parentBlockNum); 
			
			return (new InternalNode(parentBlock,true)).delete(afterBlock);
			
		}
	}  
    
	
	public int compareTo(byte[] buffer1,byte[] buffer2) {
	
		for (int i = 0, j = 0; i < buffer1.length && j < buffer2.length; i++, j++) {
			int a = (buffer1[i] & 0xff);
			int b = (buffer2[j] & 0xff);
			if (a != b) {
				return a - b;
			}
		}
		return buffer1.length - buffer2.length;
	}
	
	//delete on tree
	public void delete(byte[] originalkey){
		if (originalkey == null)    throw new NullPointerException();  

		Node rootNode;
		if(myRootBlock.values[0]=='I'){ 
			rootNode=new InternalNode(myRootBlock,true);
		}
		else{
			rootNode=new LeafNode(myRootBlock,true);
		}
	

		byte[] key=new byte[myIndexInfo.columnLength];
		
		int j=0;
		for(;j<originalkey.length;j++){
			key[j]=originalkey[j];
		}
		
	    for(;j<myIndexInfo.columnLength;j++){
			key[j]='&';
		}
		
		BufferBlock newBlock=rootNode.delete(key);
    
		if(newBlock!=null){ //if return, it means the rootblock is renewed 
			myRootBlock=newBlock;
		}
		
		CatalogManager.setIndexRoot(myIndexInfo.indexName, myRootBlock.blockOffset);
	}
	
	public void insert(byte[] originalkey,int blockOffset, int offset){
		if (originalkey == null)    throw new NullPointerException();  

		Node rootNode;
		if(myRootBlock.values[0]=='I'){ 
			rootNode=new InternalNode(myRootBlock,true);
		}
		else{
			rootNode=new LeafNode(myRootBlock,true);
		}

		byte[] key=new byte[myIndexInfo.columnLength];
		
		int j=0;
		for(;j<originalkey.length;j++){
			key[j]=originalkey[j];
		}
		
	    for(;j<myIndexInfo.columnLength;j++){
			key[j]='&';
		}
		
		BufferBlock newBlock=rootNode.insert(key, blockOffset, offset); //insert operation
    
		if(newBlock!=null){ //if return, it means the rootblock is renewed 
			myRootBlock=newBlock;
		}
		
		CatalogManager.setIndexRoot(myIndexInfo.indexName, myRootBlock.blockOffset);
	}
	
	//search on tree
	public offsetInfo searchKey(byte[] originalkey){
		Node rootNode;
		if(myRootBlock.values[0]=='I'){ 
			rootNode=new InternalNode(myRootBlock,true);
		}
		else{
			rootNode=new LeafNode(myRootBlock,true);
		}

		
		byte[] key=new byte[myIndexInfo.columnLength];
		
		int j=0;
		for(;j<originalkey.length;j++){
			key[j]=originalkey[j];
		}
		
	    for(;j<myIndexInfo.columnLength;j++){
			key[j]='&';
		}
		
		
		return rootNode.searchKey(key); 
	}
	
}