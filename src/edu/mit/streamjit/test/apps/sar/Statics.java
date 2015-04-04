package edu.mit.streamjit.test.apps.sar;

/**
 * @author sumanan
 * @since 30 Mar, 2015
 */
public final class Statics {

	private Statics() {
	}

	// getUserParameters.m
	public static final double SCALE_FACTOR = 1.0;

	// getSARparams0.m
	public static final double ASPECT_RATIO = 0.4;
	public static final double RANGE_FACTOR = 10.0;

	// [04-04-2015] L was 100 in the StreamIt version. But L=100 causes big
	// buffer sizes, longer init schedule delay and discrete output pattern.
	// So I changed it to 1 to have more smoother and continuous outputs.
	/* synthetic aperture is 2*L */
	public static final double L = 1 * SCALE_FACTOR;

	/* target area in cross-range is within */
	public static final double Y0 = L;

	/* target area in range is within [Xc-X0,Xc+X0] */
	public static final double X0 = Y0 * ASPECT_RATIO;

	/* range distance to center of target area */
	public static final double Xc = Y0 * RANGE_FACTOR;

	// getSARparams.m
	public static final double RELATIVE_BANDWIDTH = 0.5;
	public static final int RPULSE = 75;

	/* propagation speed */
	public static final double c = 3e8;

	/* carrier frequency */
	public static final double fc = 400e6;

	/* spatial/geometric parameters */
	public static final double Rmin = Xc - X0;
	public static final double Rmax = Math.sqrt(((Xc + X0) * (Xc + X0))
			+ ((Y0 + L) * (Y0 + L)));

	/* chirp pulse duration */
	public static final double Tp = RPULSE / c;

	/* baseband bandwidth */
	public static final double f0 = 0.5 * RELATIVE_BANDWIDTH * fc;

	/* wavelength at highest frequency */
	public static final double lambda_min = c / (fc + f0);
}
