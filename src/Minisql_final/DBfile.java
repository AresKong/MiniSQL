package Minisql_final;


import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.FileWriter;
import java.io.File;

public class DBfile {
	static public void BlocktoFile(String filename,byte[] content, int offset, int recordNum )throws Exception {
        
		FileWriter fileWriter5=new FileWriter("temp1.file");
		fileWriter5.write("");
		fileWriter5.flush();
        fileWriter5.close();
        FileWriter fileWriter6=new FileWriter("temp2.file");
		fileWriter6.write("");
		fileWriter6.flush();
        fileWriter6.close();
		
    	File f=new File("temp1.file");
		FileOutputStream fileWriter1= new FileOutputStream(f,true);
		File f2=new File("temp2.file");
		FileOutputStream fileWriter4= new FileOutputStream(f2,true);
		
		File fil=new File(filename);
		FileOutputStream fileWriter= new FileOutputStream(fil,true);
		
        File file = new File(filename);
        FileInputStream reader = null;
        reader = new FileInputStream(file);
        byte[] ofs=new byte[4];
        byte[] rcn=new byte[4];
        byte[] blocks=new byte[4096];
        int ofst=0;
        ofst=reader.read(ofs);
        
		if(ofst==-1){//���ݿ��ļ�Ϊ��

	        //System.out.println(ofst);
			byte off[]=new byte[4];
			off[0] = (byte)((offset >> 24) & 0xFF);
			off[1] = (byte)((offset >> 16) & 0xFF);
			off[2] = (byte)((offset >> 8) & 0xFF); 
			off[3] = (byte)(offset & 0xFF);
			
			byte rec[]=new byte[4];
			rec[0] = (byte)((recordNum >> 24) & 0xFF);
			rec[1] = (byte)((recordNum  >> 16) & 0xFF);
			rec[2] = (byte)((recordNum  >> 8) & 0xFF); 
			rec[3] = (byte)(recordNum  & 0xFF);
			/*
			int v=0;
			for(int i=0;i<4;i++){
				int shift=(4-1-i)*8;
				v+=(off[i]&0x000000FF)<<shift;
			}
			System.out.println(v);
			*/
			fileWriter.write(off);
	        fileWriter.write(rec);
	        fileWriter.write(content);
	        fileWriter.flush();
	        fileWriter.close();
	        reader.close();
	        fileWriter1.flush();
		    fileWriter1.close();
		    fileWriter4.flush();
		    fileWriter4.close();
		}
		else{//���ݿ��ļ�����
			//System.out.println(1);
			//int num=0;
			
			int int_ofs=0;
			for(int i=0;i<4;i++){
				int shift=(4-1-i)*8;
				int_ofs+=(ofs[i]&0x000000FF)<<shift;
			}
			while((int_ofs<offset)&&(ofst!=-1)){
				reader.read(rcn);
				reader.read(blocks);
				fileWriter1.write(ofs);
				fileWriter1.write(rcn);
				fileWriter1.write(blocks);
				
				ofst=reader.read(ofs);
				int_ofs=0;
				for(int i=0;i<4;i++){
					int shift=(4-1-i)*8;
					int_ofs+=(ofs[i]&0x000000FF)<<shift;
				}
			}
			
			byte off[]=new byte[4];
			off[0] = (byte)((offset >> 24) & 0xFF);
			off[1] = (byte)((offset >> 16) & 0xFF);
			off[2] = (byte)((offset >> 8) & 0xFF); 
			off[3] = (byte)(offset & 0xFF);
			
			byte rec[]=new byte[4];
			rec[0] = (byte)((recordNum >> 24) & 0xFF);
			rec[1] = (byte)((recordNum  >> 16) & 0xFF);
			rec[2] = (byte)((recordNum  >> 8) & 0xFF); 
			rec[3] = (byte)(recordNum  & 0xFF);
			////
			
			////
			fileWriter1.write(off);
		    fileWriter1.write(rec);
		    fileWriter1.write(content);//temp1�ļ�������Ҫ�滻�鼰Ҫ�滻��ǰ�����п������
		    fileWriter1.flush();
		    fileWriter1.close();
			
	        
	        if(ofst==-1){//û���ҵ�  д���ļ����
	        	fileWriter.flush();
		        fileWriter.close();
		        reader.close();
		        fileWriter4.flush();
			    fileWriter4.close();
	        }
	        else{
			//�ҵ���Ҫ�滻����һ��	
	        	//while (ofst!=-1) {//����
	        	if(int_ofs==offset){
	        		reader.read(rcn);
					reader.read(blocks);
					//num++;
					ofst=reader.read(ofs);
					if(ofst!=-1){
						int_ofs=0;
						for(int i=0;i<4;i++){
							int shift=(4-1-i)*8;
							int_ofs+=(ofs[i]&0x000000FF)<<shift;
						}
					}
			    	//if(int_ofs>=offset+1)
			    		//break;
			    }
				//if(int_ofs>=offset+1){//���ҷ��ֱ��滻�ǿ����ȥ���кܶ��
					while(ofst!=-1){
						reader.read(rcn);
						reader.read(blocks);
						fileWriter4.write(ofs);
						fileWriter4.write(rcn);
						fileWriter4.write(blocks);
						ofst=reader.read(ofs);
					}
				// }
	        	reader.close();
			    fileWriter4.flush();
				fileWriter4.close();
	        }
	        FileWriter fileWriter2=new FileWriter(filename);//������ݿ��ļ�
			fileWriter2.write("");
			fileWriter2.flush();
	        fileWriter2.close();
	        
	        File f3=new File(filename);
			FileOutputStream fileWriter3=new FileOutputStream(f3,true);
	        File file3 = new File("temp1.file");
	        FileInputStream reader3 = null;
	        reader3 = new FileInputStream(file3);
	        byte[] a=new byte[4];
	        byte[] b=new byte[4];
	        byte[] c=new byte[4096];
	        int jd=0;
	        jd=reader3.read(a);
	        while(jd!=-1){
				reader3.read(b);
				reader3.read(c);
				fileWriter3.write(a);
				fileWriter3.write(b);
				fileWriter3.write(c);
				jd=reader3.read(a);
			}
	        reader3.close();
	        //////////////////////////////
	        
	        File file4 = new File("temp2.file");
	        FileInputStream reader4 = null;
	        reader4 = new FileInputStream(file4);
	        jd=reader4.read(a);
	        while (jd!=-1) {
	        	reader4.read(b);
				reader4.read(c);
				fileWriter3.write(a);
				fileWriter3.write(b);
				fileWriter3.write(c);
				jd=reader4.read(a);
	    	}
	        reader4.close();
	        fileWriter3.flush();
	        fileWriter3.close();
		}
		
		return ; 
	}
	static public BufferBlock FiletoBlock(String filename,int offset)throws Exception {
		
		BufferBlock b = new BufferBlock();
		File file = new File(filename);
		FileInputStream reader = null;
		reader = new FileInputStream(file);
		byte[] ofs=new byte[4];
	    byte[] rcn=new byte[4];
	    byte[] blocks=new byte[4096];
	    int ofst=0;
	    ofst=reader.read(ofs);
	    int int_ofs=0;
    	
		if(ofst==-1){
			b.recordNum=0;
			//b.values=null;
			reader.close();
			return b;
		}
		else{
			int_ofs=0;
			for(int i=0;i<4;i++){
				int shift=(4-1-i)*8;
				int_ofs+=(ofs[i]&0x000000FF)<<shift;
			}
			while(int_ofs!=offset){
				reader.read(rcn);
				reader.read(blocks);
				int_ofs++;
				ofst=reader.read(ofs);
			}
			reader.read(rcn);
			reader.read(blocks);
			
			for(int i=0;i<4;i++){
				int shift=(4-1-i)*8;
				b.recordNum+=(rcn[i]&0x000000FF)<<shift;
			}
			b.values=blocks;
			reader.close();
			return b;
		}
	}
}
