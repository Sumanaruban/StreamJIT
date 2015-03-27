package edu.mit.streamjit.transmitter;

public class Modulator extends edu.mit.streamjit.api.Filter<Byte, Float> {
	
	public Modulator() {
		super(6, 2);
	}

	@Override
	public void work() {
		float[] real={-7,-7,-7,-7,-7,-7,-7,-7,-5,-5,-5,-5,-5,-5,-5,-5,-1,-1,-1,-1,-1,-1,-1,-1,-3,-3,-3,-3,-3,-3,-3,-3,7,7,7,7,7,7,7,7,5,5,5,5,5,5,5,5,1,1,1,1,1,1,1,1,3,3,3,3,3,3,3,3};
		float[] complex={-7,-5,-1,-3,7,5,1,3,-7,-5,-1,-3,7,5,1,3,-7,-5,-1,-3,7,5,1,3,-7,-5,-1,-3,7,5,1,3,-7,-5,-1,-3,7,5,1,3,-7,-5,-1,-3,7,5,1,3,-7,-5,-1,-3,7,5,1,3,-7,-5,-1,-3,7,5,1,3};
		
		int val=0;
		for (int i = 0; i <6; i++) {
			if(pop()==1)	val=val+(int)Math.pow(2, i);
		}
		
		push(real[val]);
		push(complex[val]);
		
	}
	

}
