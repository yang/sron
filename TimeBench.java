public class TimeBench {

	public static void main(String[] args) {
		// On my dual-core Intel machine, millis is *faster* than nano, despite the
		// fact that FreeNode folk told me nano was more accurate (perhaps it is
		// once you get to that granularity, but the cost of invocation is twice as
		// high):
		//
		// $ for i in {1..10}; do java TimeBench; done
		// 213
		// 416283169
		// 210
		// 427611823
		// 211
		// 425782976
		// 211
		// 416913721
		// 213
		// 419326076
		// 209
		// 415857948
		// 209
		// 415659440
		// 210
		// 410968289
		// 212
		// 421450138
		// 216
		// 417043555
		//
		// Furthermore, I verified using strace, that none of these produces
		// syscalls.

		{
			long t = System.currentTimeMillis(), t0 = System.currentTimeMillis();
			for (int i = 0; i < 1000000; i++)
				t = System.currentTimeMillis();
			System.out.println(t - t0);
		}

		{
			long t = System.nanoTime(), t0 = System.nanoTime();
			for (int i = 0; i < 1000000; i++)
				t = System.nanoTime();
			System.out.println(t - t0);
		}

	}

}
