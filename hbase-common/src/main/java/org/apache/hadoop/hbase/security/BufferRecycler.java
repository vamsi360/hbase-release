/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.security;

import java.lang.ref.SoftReference;

/**
 * Simple helper class to encapsulate details of basic buffer recycling scheme, which helps a lot
 * (as per profiling) for smaller encoding cases. Based on techniques used in Ning's
 * {@code compress-lzf} and Jackson.
 */
public class BufferRecycler {
    private final static int MIN_BUFFER_SIZE = 512;
    private static int MAX_BUFFER_SIZE = Integer.MAX_VALUE;

    /**
     * This {@link ThreadLocal} contains a {@link SoftReference} to a
     * {@link BufferRecycler} used to provide a low-cost buffer recycling for buffers we need for
     * encoding and decoding.
     */
    private static final ThreadLocal<SoftReference<BufferRecycler>> RECYCLER = new ThreadLocal<>();

    private byte[] buffer;

    /**
     * Sets a limit on size of buffers retained per thread
     *
     * @param maxBufferSize maximum retained buffer size in bytes
     */
     public static void setGlobalMaxBufferSize(int maxBufferSize) {
        MAX_BUFFER_SIZE = maxBufferSize;
     }

    /**
     * Returns the {@link BufferRecycler} instance for the calling thread.
     *
     * @return a thread-local instance of {@link BufferRecycler}
     */
    public static BufferRecycler recycler() {
        SoftReference<BufferRecycler> ref = RECYCLER.get();
        BufferRecycler br = (ref == null) ? null : ref.get();
        if (br == null) {
            br = new BufferRecycler();
            RECYCLER.set(new SoftReference<>(br));
        }
        return br;
    }

    /**
     * Allocates a new byte array or reuses an old one with at least as many bytes as the specified
     * minimum.
     *
     * @param minSize the minimum number of bytes in the returned array
     * @return an array of at least {@code minSize} length
     */
    public byte[] allocate(int minSize) {
        byte[] buf = buffer;
        if (buf == null || buf.length < minSize) {
            buf = new byte[Math.max(minSize, MIN_BUFFER_SIZE)];
        } else {
            reset();
        }
        return buf;
    }

    /**
     * Returns an unused byte array back to the pool for reuse.
     *
     * @param buffer a byte array which is no longer in use
     */
    public void release(byte[] buffer) {
        if (this.buffer == null || buffer.length > this.buffer.length) {
            if (buffer.length < MAX_BUFFER_SIZE) {
                this.buffer = buffer;
            }
        }
    }

    /**
     * Reset the internal buffer to {@code null}.
     */
    public void reset() {
        this.buffer = null;
    }
}
