package edu.mit.streamjit.transmitter;

import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;

public class TurboEncoder extends edu.mit.streamjit.api.Pipeline<Byte, Byte> {
	static int val=0;
	public TurboEncoder(){
		super(	
				new Permutator(),
				new Splitjoin<Byte,Byte>(
						new RoundrobinSplitter<Byte>(), 
						new RoundrobinJoiner<Byte>(), 
						new Output1(),
						new Output2(),
						new Output3()
						),
				new Interleaver()
//				new Printer()
		);
	}
	
	
		
	private static class Output1 extends
	edu.mit.streamjit.api.Filter<Byte, Byte> {
		
		public Output1() {
			super(1, 1);
		}

		@Override
		public void work() {
			byte a = pop();
			
			push(a);

		}
	}
	
	private static class Output2 extends
	edu.mit.streamjit.api.Filter<Byte, Byte> {
		
		byte[] d={0, 0 , 0 , 0};
		byte[] weights={1 ,0 , 1 , 1};
		
		public Output2() {
			super(1, 1);
		}

		@Override
		public void work() {
			d[0] = pop();
			
			byte out=0;
			
			for (int i = 0; i < d.length; i++) {
				out=(byte)(out^(d[i]*weights[i]));
			}
			
			push(out);
			
			for (int i =d.length-1; i>0; i--) {
				d[i]=d[i-1];				
			}
		}
	}
	
	
	private static class Output3 extends
	edu.mit.streamjit.api.Filter<Byte, Byte> {
		
		byte[] d={0, 0 , 0 , 0};
		byte[] weights={1 ,1 , 0 , 1};
		
		public Output3() {
			super(1, 1);
		}

		@Override
		public void work() {
			d[0] = pop();
			
			byte out=0;
			
			for (int i = 0; i < d.length; i++) {
				out=(byte)(out^(d[i]*weights[i]));
			}
			
			push(out);
			
			for (int i =d.length-1; i>0; i--) {
				d[i]=d[i-1];				
			}
			
		}
	}
	
	
	private static class Stuffer extends
	edu.mit.streamjit.api.Filter<Byte, Byte> {
		
		int columns=36;
		int rows=8;
		byte[][] block=new byte[rows][columns];
		public Stuffer() {
			super(283,864);
		}

		@Override
		public void work() {
			
			byte[] linear1=new byte[rows*columns];
			byte[] linear2=new byte[rows*columns];
			
			for (int i = 0; i <rows; i++) {
				for (int j = 0; j <columns; j++) {
					if(i>rows-4&&j==columns-1){
						block[i][j]=0;
					}else if(j>columns-4&&i==rows-1){
						block[i][j]=0;
					}else{
						block[i][j]=pop();						
					}
					
				}
			}			
					
			
			for (int i = 0; i < rows; i++) {
				for (int j = 0; j < columns; j++) {
					linear1[i*columns+j]=block[i][j];
				}
			}
			
			for (int i = 0; i < columns; i++) {
				for (int j = 0; j < rows; j++) {
					linear2[i*rows+j]=block[j][i];
				}
			}
			
			for (int i = 0; i <rows*columns; i++) {
				push(linear1[i]);
				push(linear1[i]);
				push(linear2[i]);
			}

		}
	}
	
	private static class Permutator extends
	edu.mit.streamjit.api.Filter<Byte, Byte> {
		
		int columns=36;
		int rows=8;
		byte[][] block=new byte[rows][columns];
		public Permutator() {
			super(40,80);
		}

		@Override
		public void work() {
			for (int i = 0; i < 40; i++) {
				byte a=pop();
				push(a);
				push(a);
			}
			
			
		}
	}
	
	private static class Interleaver extends
	edu.mit.streamjit.api.Filter<Byte, Byte> {
		
		int columns=96;
		int rows=9;
		byte[][] block=new byte[rows][columns];
		public Interleaver() {
			super(864,864);
		}

		@Override
		public void work() {
//			val++;
//			System.out.println(val);
						
			for (int i = 0; i <rows; i++) {
				for (int j = 0; j <columns; j++) {
					block[i][j]=pop();						
				}
			}								
				
			
			for (int i = 0; i < columns; i++) {
				for (int j = 0; j < rows; j++) {
					push(block[j][i]);
				}
			}
			
		}
	}
	
	private static class Printer extends
	edu.mit.streamjit.api.Filter<Byte, Byte> {
		
		public Printer() {
			super(1, 1);
		}

		@Override
		public void work() {
			byte a = pop();
			System.out.println(a);
			push(a);

		}
	}
	
}

