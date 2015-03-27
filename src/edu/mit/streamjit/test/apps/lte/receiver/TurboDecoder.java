package edu.mit.streamjit.receiver;



import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;
import edu.mit.streamjit.api.WeightedRoundrobinJoiner;
import edu.mit.streamjit.api.WeightedRoundrobinSplitter;

public class TurboDecoder extends edu.mit.streamjit.api.Pipeline<Byte, Byte> {
	
	static int block_size=40;
	static int[] zero_transitions={0,4,5,1,2,6,7,3};
	static int[] one_transitions={4,0,1,5,6,2,3,7};
	static int[] zero_transitions_gammas={0,1,1,0,0,1,1,0};
	static int[] one_transitions_gammas={3,2,2,3,3,2,2,3};
	static int[] reverse_zero_transitions={0,3,4,7,1,2,5,6};
	static int[] reverse_one_transitions={1,2,5,6,0,3,4,7};
	
	static int[] pos_1={0,4};
	static int[] pos_2={0,2,4,6};
	static int[] pos_41={0,1,2,3};
	static int[] pos_42={0,1};
	
	static int[] perms=new int[40];
	static int[] reverse_perms=new int[40];
	static int f1=3;
	static int f2=10;
	
	static float Lc=5.0f;
	
	public TurboDecoder(){
		super(	new PolarEncoder(),
				new Divider(),
				new Splitjoin<Byte,Float>(
						new RoundrobinSplitter<Byte>(86),
						new RoundrobinJoiner<Float>(80),
						new Lprime(),
						new Lprime()
						),
				new Connector(),
				new Decoder1(),
				new Decoder2(),
				new Decoder1(),
				new Decoder2(),
				new Decoder1(),
				new Decoder2(),
				new Decoder1(),
				new Decoder2(),
				new Decoder1(),
				new Decoder2()
			);
		
		for (int i = 0; i < perms.length; i++) {
			int val=(f1*i+f2*i*i)%block_size;
			perms[i]=val;
			reverse_perms[val]=i;
		}
		
		
//		super(	new Stuffer(),
//				new Splitjoin<Byte,Byte>(
//						new RoundrobinSplitter<Byte>(), 
//						new RoundrobinJoiner<Byte>(), 
//						new Output1(),
//						new Output2(),
//						new Output3()
//						),
//				new Interleaver()
////				new Printer()
//		);
	}
	
	
		
	private static class PolarEncoder extends
	edu.mit.streamjit.api.Filter<Byte, Byte> {
		
		public PolarEncoder() {
			super(1, 1);
		}

		@Override
		public void work() {
					
			
			if(pop()==0)	push((byte)-1);
			else 			push((byte)1);

		}
	}
	
	private static class Divider extends
	edu.mit.streamjit.api.Filter<Byte, Byte> {
		byte[] data1=new byte[block_size+3];
		byte[] data2=new byte[block_size+3];
		byte[] parity1=new byte[block_size+3];
		byte[] parity2=new byte[block_size+3];
		
		
		public Divider() {
			super(132,172);
		}

		@Override
		public void work() {
			for (int i = 0; i < 40; i++) {
				data1[i]=pop();
				data2[perms[i]]=data1[i];
				parity1[i]=pop();
				parity2[i]=pop();
				
			}
			for (int i =40; i <43; i++) {
				data1[i]=pop();
				parity1[i]=pop();
			}
			for (int i =40; i <43; i++) {
				data2[i]=pop();
				parity2[i]=pop();
			}
			
			for (int i = 0; i <43; i++) {
				push(data1[i]);
				push(parity1[i]);
			}
			
			for (int i = 0; i <43; i++) {
				push(data2[i]);
				push(parity2[i]);
			}

		}
	}
	
	private static class Lprime extends
	edu.mit.streamjit.api.Filter<Byte, Float> {
		
		
		public Lprime() {
			super(86, 80);
		}

		@Override
		public void work() {
			Byte[] y=new Byte[43];
			Byte[] parity=new Byte[43];
			
			for (int i = 0; i < y.length; i++) {
				y[i]=pop();
				parity[i]=pop();
			}
			
			float[][] alpha=new float[8][44]; 
			float[][] beta=new float[8][44];
			float[][] gamma=new float[4][44];
			
			//gamma calculation
			
			for (int i = 1; i < 43; i++) {
				gamma[0][i]=Lc/2*(-1*parity[i]-1*y[i]);
				gamma[1][i]=Lc/2*(-1*parity[i]+1*y[i]);
				gamma[2][i]=Lc/2*(1*parity[i]-1*y[i]);
				gamma[3][i]=Lc/2*(1*parity[i]+1*y[i]);
			}
			
			
			//alpha update		
			
			alpha[0][0]=0;
			
			for (int i = 0; i < pos_1.length; i++) {
				int pos=1;
				int current=pos_1[i];
				
				int zero_pos=reverse_zero_transitions[current];
				int one_pos=reverse_one_transitions[current];
				
				alpha[current][pos]=Utilities.max_e(alpha[zero_pos][pos-1]+gamma[zero_transitions_gammas[current]][pos],alpha[one_pos][pos-1]+gamma[one_transitions_gammas[current]][pos]);
				
			}
			
			for (int i = 0; i < pos_2.length; i++) {
				int pos=2;
				int current=pos_2[i];
				
				int zero_pos=reverse_zero_transitions[current];
				int one_pos=reverse_one_transitions[current];
				
				alpha[current][pos]=Utilities.max_e(alpha[zero_pos][pos-1]+gamma[zero_transitions_gammas[current]][pos],alpha[one_pos][pos-1]+gamma[one_transitions_gammas[current]][pos]);
				
			}
			
			for (int i = 3; i < 41; i++) {
				int pos=i;
				for (int j = 0; j <8; j++) {
					int current=j;
					
					int zero_pos=reverse_zero_transitions[current];
					int one_pos=reverse_one_transitions[current];
					
					alpha[current][pos]=Utilities.max_e(alpha[zero_pos][pos-1]+gamma[zero_transitions_gammas[current]][pos],alpha[one_pos][pos-1]+gamma[one_transitions_gammas[current]][pos]);
				}
			}
			
			
			//beta update		
			
			beta[0][43]=0;
			
			for (int i = 0; i < pos_42.length; i++) {
				int pos=42;
				int current=pos_42[i];
				
				int zero_pos=zero_transitions[current];
				int one_pos=one_transitions[current];
				
				beta[current][pos]=Utilities.max_e(beta[zero_pos][pos+1]+gamma[zero_transitions_gammas[zero_pos]][pos+1],beta[one_pos][pos+1]+gamma[one_transitions_gammas[one_pos]][pos+1]);
				
			}
			
			for (int i = 0; i < pos_41.length; i++) {
				int pos=41;
				int current=pos_41[i];
				
				int zero_pos=zero_transitions[current];
				int one_pos=one_transitions[current];
				
				beta[current][pos]=Utilities.max_e(beta[zero_pos][pos+1]+gamma[zero_transitions_gammas[zero_pos]][pos+1],beta[one_pos][pos+1]+gamma[one_transitions_gammas[one_pos]][pos+1]);
				
			}
			
			for (int i = 40; i >2; i--) {
				int pos=i;
				for (int j = 0; j <8; j++) {
					int current=j;
					
					int zero_pos=zero_transitions[current];
					int one_pos=one_transitions[current];
					
					beta[current][pos]=Utilities.max_e(beta[zero_pos][pos+1]+gamma[zero_transitions_gammas[zero_pos]][pos+1],beta[one_pos][pos+1]+gamma[one_transitions_gammas[one_pos]][pos+1]);
				}
			}
			
			for (int i = 0; i < pos_2.length; i++) {
				int pos=2;
				int current=pos_2[i];
				
				int zero_pos=zero_transitions[current];
				int one_pos=one_transitions[current];
				
				beta[current][pos]=Utilities.max_e(beta[zero_pos][pos+1]+gamma[zero_transitions_gammas[zero_pos]][pos+1],beta[one_pos][pos+1]+gamma[one_transitions_gammas[one_pos]][pos+1]);
				
			}
			
			for (int i = 0; i < pos_1.length; i++) {
				int pos=1;
				int current=pos_1[i];
				
				int zero_pos=zero_transitions[current];
				int one_pos=one_transitions[current];
				
				beta[current][pos]=Utilities.max_e(beta[zero_pos][pos+1]+gamma[zero_transitions_gammas[zero_pos]][pos+1],beta[one_pos][pos+1]+gamma[one_transitions_gammas[one_pos]][pos+1]);
				
			}
			
			for (int i = 0; i < 1; i++) {
				int pos=0;
				int current=0;
				
				int zero_pos=zero_transitions[current];
				int one_pos=one_transitions[current];
				
				beta[current][pos]=Utilities.max_e(beta[zero_pos][pos+1]+gamma[zero_transitions_gammas[zero_pos]][pos+1],beta[one_pos][pos+1]+gamma[one_transitions_gammas[one_pos]][pos+1]);
				
			}
			
			//LLR calculation
			
			push(alpha[0][0]+gamma[zero_transitions_gammas[0]][1]+beta[0][1]-alpha[0][0]+gamma[zero_transitions_gammas[4]][1]+beta[4][1]);
			push((float)y[0]);
			
			float[] ones=new float[2];
			float[] zeros=new float[2];
			for (int j = 0; j <pos_1.length; j++) {
				int pos=1;
				int current=pos_1[j];
				
				int zero_pos=zero_transitions[current];
				int one_pos=one_transitions[current];
				
				zeros[j]=alpha[current][pos]+gamma[zero_transitions_gammas[zero_pos]][pos+1]+beta[zero_pos][pos+1];
				ones[j]=alpha[current][pos]+gamma[one_transitions_gammas[one_pos]][pos+1]+beta[one_pos][pos+1];
			}
			
			push(Utilities.max_e(ones)-Utilities.max_e(zeros));
			push((float)y[1]);
			
			ones=new float[4];
			zeros=new float[4];
			for (int j = 0; j <pos_2.length; j++) {
				int pos=2;
				int current=pos_2[j];
				
				int zero_pos=zero_transitions[current];
				int one_pos=one_transitions[current];
				
				zeros[j]=alpha[current][pos]+gamma[zero_transitions_gammas[zero_pos]][pos+1]+beta[zero_pos][pos+1];
				ones[j]=alpha[current][pos]+gamma[one_transitions_gammas[one_pos]][pos+1]+beta[one_pos][pos+1];
			}
			
			push(Utilities.max_e(ones)-Utilities.max_e(zeros));
			push((float)y[2]);
			
			
			
			
			for (int i = 3; i <40; i++) {
				int pos=i;
				ones=new float[8];
				zeros=new float[8];
								
				for (int j = 0; j <8; j++) {
					int current=j;
					
					int zero_pos=zero_transitions[current];
					int one_pos=one_transitions[current];
					
					zeros[j]=alpha[current][pos]+gamma[zero_transitions_gammas[zero_pos]][pos+1]+beta[zero_pos][pos+1];
					ones[j]=alpha[current][pos]+gamma[one_transitions_gammas[one_pos]][pos+1]+beta[one_pos][pos+1];
				}
				float temp=Utilities.max_e(ones)-Utilities.max_e(zeros);
				System.out.println(temp);
				push(temp);
				push((float)y[i]);
			}
			
			
		}
	}
	
	
	private static class Dummy extends
	edu.mit.streamjit.api.Filter<Byte, Byte> {
		int r;		
		public Dummy(int rate) {
			super(rate, rate);
			r=rate;
		}

		@Override
		public void work() {
			for (int i = 0; i < r; i++) {
				push(pop());
			}
						
		}
	}
	
	private static class Connector extends
	edu.mit.streamjit.api.Filter<Float, Float> {
				
		public Connector() {
			super(160, 200);			
		}

		@Override
		public void work() {
			for (int i = 0; i < 40; i++) {
				push(pop());
				push(pop());
				push(0f);
			}
			for (int i = 0; i < 40; i++) {
				push(pop());
				push(pop());				
			}
						
		}
	}
	
	private static class Decoder1 extends
	edu.mit.streamjit.api.Filter<Float, Float> {
		
		float[] L=new float[40];
		float[] LLR=new float[40];
		float[] y=new float[40];
		
		public Decoder1() {
			super(200, 200);
			
		}

		@Override
		public void work() {
			
			for (int i = 0; i <40; i++) {
				LLR[i]=pop();
				y[i]=pop();
				float prev=pop();
				L[perms[i]]=LLR[i]-y[i]*Lc-prev;
								
			}
			
			for (int i = 0; i <40; i++) {
				push(pop());
				push(pop());
				push(L[i]);
			}
			
			for (int i = 0; i <40; i++) {
				push(LLR[i]);
				push(y[i]);				
			}
			
			
		}
	}
	
	private static class Decoder2 extends
	edu.mit.streamjit.api.Filter<Float, Float> {
		
		float[] L=new float[40];
		float[] LLR=new float[40];
		float[] y=new float[40];
		
		public Decoder2() {
			super(200, 200);
			
		}

		@Override
		public void work() {
			
			for (int i = 0; i <40; i++) {
				LLR[i]=pop();
				y[i]=pop();
				float prev=pop();
				L[reverse_perms[i]]=LLR[i]-y[i]*Lc-prev;
								
			}
			
			for (int i = 0; i <40; i++) {
				push(pop());
				push(pop());
				push(L[i]);
			}
			
			for (int i = 0; i <40; i++) {
				push(LLR[i]);
				push(y[i]);				
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

