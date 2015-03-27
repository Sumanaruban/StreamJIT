package edu.mit.streamjit.transmitter;

import edu.mit.streamjit.api.RoundrobinJoiner;
import edu.mit.streamjit.api.RoundrobinSplitter;
import edu.mit.streamjit.api.Splitjoin;


public class AntennaArray extends edu.mit.streamjit.api.Pipeline<Float, Float>{
	static int fftlength=6;	
	static int stuff=1;
	public AntennaArray(){				
		super(new Splitjoin<Float,Float>(
				new RoundrobinSplitter<Float>(fftlength*2),
				new RoundrobinJoiner<Float>((fftlength+2*stuff)*2),
				new Antenna(),
				new Antenna()
				)
//			new Printer()
		);
	}


	private static class FFT  extends
	edu.mit.streamjit.api.Filter<Float, Float> {
		int len=1;
		public FFT(int length) {
			super(length*2,length*2);
			len=length;
		}

		@Override
		public void work() {
			for (int i = 0; i < len; i++) {
				push(pop());
				push(pop());				
			}
		}
		
	}
	
	
	private static class IFFTPrepare  extends
	edu.mit.streamjit.api.Filter<Float, Float> {
			
		
		public IFFTPrepare() {
			super(fftlength*2, fftlength*2+stuff*2*2);
					
		}

		@Override
		public void work() {
			for (int i = 0; i < stuff; i++) {
				push(0f);
				push(0f);
				
			}
			
			for (int i = 0; i < fftlength; i++) {
				push(pop());
				push((-1*pop()));
				
			}
			
			for (int i = 0; i < stuff; i++) {
				push(0f);
				push(0f);
				
			}

		}
	}
	
	private static class Antenna  extends
	edu.mit.streamjit.api.Pipeline<Float, Float> {
		
		public Antenna() {
			super(new FFT(fftlength),new IFFTPrepare(),new FFT(fftlength+2*stuff),new ScaleConjugate(),new CP());
		}

		
	}
	
	private static class ScaleConjugate  extends
	edu.mit.streamjit.api.Filter<Float, Float> {
		
		public ScaleConjugate() {
			super(2,2);
		}

		@Override
		public void work() {
			push(pop()/(fftlength+2*stuff));
			push(-1*pop()/(fftlength+2*stuff));

		}
	}
	
	private static class CP  extends
	edu.mit.streamjit.api.Filter<Float, Float> {
		
		public CP() {
			super(1,1);
		}

		@Override
		public void work() {
			push(pop());

		}
	}
	
	private static class Printer extends
	edu.mit.streamjit.api.Filter<Float, Float> {
		
		public Printer() {
			super(1, 1);
		}

		@Override
		public void work() {
			float a = pop();
			System.out.println(a);
			push(a);

		}
	}
	
}
