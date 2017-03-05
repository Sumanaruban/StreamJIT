/*
 * Copyright (c) 2013-2014 Massachusetts Institute of Technology
 *
 * Permission is hereby granted, free of charge, to any person obtaining a copy
 * of this software and associated documentation files (the "Software"), to deal
 * in the Software without restriction, including without limitation the rights
 * to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
 * copies of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 *
 * The above copyright notice and this permission notice shall be included in
 * all copies or substantial portions of the Software.
 *
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
 * IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
 * FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
 * AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
 * LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
 * OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
 * THE SOFTWARE.
 */
package edu.mit.streamjit.api;

import static com.google.common.base.Preconditions.checkArgument;

import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.lang.invoke.MethodHandles;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.Collection;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.function.Consumer;

import com.google.common.primitives.Primitives;

import edu.mit.streamjit.impl.blob.AbstractWriteOnlyBuffer;
import edu.mit.streamjit.impl.blob.Buffer;
import edu.mit.streamjit.impl.blob.Buffers;
import edu.mit.streamjit.impl.common.OutputBufferFactory;

/**
 *
 * @author Jeffrey Bosboom <jbosboom@csail.mit.edu>
 * @since 8/17/2013
 */
public class Output<O> {
	static {
		OutputBufferFactory.OUTPUT_LOOKUP = MethodHandles.lookup();
	}
	private final OutputBufferFactory output;
	private Output(OutputBufferFactory output) {
		this.output = output;
	}

	@Override
	public String toString() {
		return output.toString();
	}

	public static final class ManualOutput<O> extends Output<O> {
		//TODO: volatile?
		private volatile Buffer buffer;
		private ManualOutput(OutputBufferFactory output) {
			super(output);
		}
		private static <O> ManualOutput<O> create() {
			class ManualRealOutput extends OutputBufferFactory {
				private ManualOutput<?> manualOutput;
				@Override
				public Buffer createWritableBuffer(int writerMinSize) {
					Buffer buf = Buffers.blockingQueueBuffer(new ArrayBlockingQueue<>(writerMinSize), false, false);
					manualOutput.buffer = buf;
					return buf;
				}
				@Override
				public String toString() {
					return "Output.createManualOutput()";
				}
			}
			ManualRealOutput mro = new ManualRealOutput();
			ManualOutput<O> mo = new ManualOutput<>(mro);
			mro.manualOutput = mo;
			return mo;
		}
		@SuppressWarnings("unchecked")
		public O poll() {
			return (O)buffer.read();
		}
		public int poll(O[] data, int offset, int length) {
			return buffer.read(data, offset, length);
		}
	}

	public static <O> ManualOutput<O> createManualOutput() {
		return ManualOutput.create();
	}

	public static <O> Output<O> blackHole() {
		return new Output<>(new OutputBufferFactory() {
			@Override
			public Buffer createWritableBuffer(final int writerMinSize) {
				return new AbstractWriteOnlyBuffer() {
					@Override
					public boolean write(Object t) {
						return true;
					}
				};
			}
		});
	}

	//TODO: we need flush() for good performance, and close() to avoid leaks.
//	public static <O> Output<O> toBinaryFile(Path path, Class<I> type) {
//
//	}

	public static <O> Output<O> toCollection(final Collection<? super O> coll) {
		return new Output<>(new OutputBufferFactory() {
			@Override
			public Buffer createWritableBuffer(int writerMinSize) {
				return new AbstractWriteOnlyBuffer() {
					@Override
					@SuppressWarnings("unchecked")
					public boolean write(Object t) {
						coll.add((O)t);
						return true;
					}
				};
			}
		});
	}

	public static <O> Output<O> toPrintStream(final PrintStream stream) {
		return new Output<>(new OutputBufferFactory() {
			@Override
			public Buffer createWritableBuffer(int writerMinSize) {
				return new AbstractWriteOnlyBuffer() {
					@Override
					public boolean write(Object t) {
						stream.println(t);
						return true;
					}
				};
			}
		});
	}

	/**
	 * Only primitives can be written to a binary file. The return object of
	 * this method call, BinaryFileOutput, must be closed properly. Use
	 * {@link BinaryFileOutput#close()} to close the file.
	 * 
	 * @param fc
	 * @param type
	 * @param byteOrder
	 * @return
	 */
	public static <O> BinaryFileOutput<O> toBinaryFile(String path,
			Class<O> type) {
		checkArgument(
				Primitives.isWrapperType(type) && !type.equals(Void.class),
				"not a wrapper type: %s", type);
		return new BinaryFileOutput<>(new BinaryFileOutputFactory(path, type));
	}

	public static final class BinaryFileOutput<O> extends Output<O> {
		private final FileOutputBufferFactory output;
		private BinaryFileOutput(FileOutputBufferFactory output) {
			super(output);
			this.output = output;
		}

		public void close() {
			output.close();
		}
	}

	private static abstract class FileOutputBufferFactory
			extends
				OutputBufferFactory {
		public abstract void close();
	}

	private static class BinaryFileOutputFactory
			extends
				FileOutputBufferFactory {
		FileChannel fc;
		ByteBuffer buffer;
		private final String path;
		private final Class<?> type;
		private final Consumer<Object> consumer;
		private final int objectSize;

		public BinaryFileOutputFactory(String path, Class<?> type) {
			this.path = path;
			this.type = type;
			if (type == Byte.class) {
				consumer = (t) -> buffer.put((byte) t);
				objectSize = Byte.BYTES;
			} else if (type == Short.class) {
				consumer = (t) -> buffer.putShort((short) t);
				objectSize = Short.BYTES;
			} else if (type == Character.class) {
				consumer = (t) -> buffer.putChar((char) t);
				objectSize = Character.BYTES;
			} else if (type == Integer.class) {
				consumer = (t) -> buffer.putInt((int) t);
				objectSize = Integer.BYTES;
			} else if (type == Long.class) {
				consumer = (t) -> buffer.putLong((long) t);
				objectSize = Long.BYTES;
			} else if (type == Float.class) {
				consumer = (t) -> buffer.putFloat((float) t);
				objectSize = Float.BYTES;
			} else if (type == Double.class) {
				consumer = (t) -> buffer.putDouble((double) t);
				objectSize = Double.BYTES;
			} else
				throw new IllegalArgumentException(
						"Unsupported BinayOutput Type" + type.getSimpleName());
		}

		@Override
		public Buffer createWritableBuffer(int readerMinSize) {
			init();
			return new AbstractWriteOnlyBuffer() {
				@Override
				public boolean write(Object t) {
					if (buffer.remaining() < objectSize)
						flush();
					consumer.accept(t);
					return true;
				}
			};
		}

		private void init() {
			try {
				fc = new FileOutputStream(path).getChannel();
			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}
			buffer = ByteBuffer.allocateDirect(32_000);
			buffer.clear();
		}

		private void flush() {
			buffer.flip();
			try {
				fc.write(buffer);
			} catch (IOException e) {
				e.printStackTrace();
			}
			buffer.clear();
		}

		@Override
		public String toString() {
			return "Output.toBinaryFile(" + ", " + type.getSimpleName()
					+ ".class, " + ")";
		}

		public void close() {
			flush();
			try {
				fc.close();
			} catch (IOException e) {
				e.printStackTrace();
			}
		}
	}
}
