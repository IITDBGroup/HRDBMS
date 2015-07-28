package com.exascale.misc;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.channels.ReadableByteChannel;
import java.nio.channels.WritableByteChannel;

/**
 * Puts a buffer in front of a {@link ReadableByteChannel} so that even small reads,
 * byte/int/long will be fast.
 */
public class BufferedFileChannel extends FileChannel
{
    private final FileChannel source;
    private final byte[] intermediaryBuffer = new byte[1024*64];
    private int intermediaryBufferSize;
    private int intermediaryBufferPosition;

    public BufferedFileChannel( FileChannel source ) throws IOException
    {
        this.source = source;
        intermediaryBufferPosition = 0;
        intermediaryBufferSize = 0;
    }

    @Override
    public int read( ByteBuffer dst ) throws IOException
    {
        int read = 0;
        while ( read < dst.limit() )
        {
            read += readAsMuchAsPossibleFromIntermediaryBuffer( dst );
            if ( read < dst.limit() )
            {
                if ( fillUpIntermediaryBuffer() == -1 )
                {
                    break;
                }
            }
        }
        return read == 0 && dst.limit() > 0 ? -1 : read;
    }

    private int readAsMuchAsPossibleFromIntermediaryBuffer( ByteBuffer dst )
    {
        int howMuchToRead = Math.min( dst.remaining(), remainingInIntermediaryBuffer() );
        dst.put( intermediaryBuffer, intermediaryBufferPosition, howMuchToRead );
        intermediaryBufferPosition += howMuchToRead;
        return howMuchToRead;
    }
    
    private int remainingInIntermediaryBuffer()
    {
        return intermediaryBufferSize-intermediaryBufferPosition;
    }

    private int fillUpIntermediaryBuffer() throws IOException
    {
        int result = source.read( ByteBuffer.wrap( intermediaryBuffer ) );
        intermediaryBufferPosition = 0;
        intermediaryBufferSize = result == -1 ? 0 : result;
        return result;
    }

    @Override
    public long read( ByteBuffer[] dsts, int offset, int length ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int write( ByteBuffer src ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long write( ByteBuffer[] srcs, int offset, int length ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long position() throws IOException
    {
        return source.position() - intermediaryBufferSize + intermediaryBufferPosition;
    }

    @Override
    public FileChannel position( long newPosition ) throws IOException
    {
        long bufferEndPosition = source.position();
        long bufferStartPosition = bufferEndPosition - intermediaryBufferSize;
        if ( newPosition >= bufferStartPosition && newPosition <= bufferEndPosition )
        {
            // Only an optimization
            long diff = newPosition-position();
            intermediaryBufferPosition += diff;
        }
        else
        {
            source.position( newPosition );
            fillUpIntermediaryBuffer();
        }
        return this;
    }

    @Override
    public long size() throws IOException
    {
        return source.size();
    }

    @Override
    public FileChannel truncate( long size ) throws IOException
    {
        source.truncate( size );
        return this;
    }

    @Override
    public void force( boolean metaData ) throws IOException
    {
        source.force( metaData );
    }

    @Override
    public long transferTo( long position, long count, WritableByteChannel target )
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public long transferFrom( ReadableByteChannel src, long position, long count )
            throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int read( ByteBuffer dst, long position ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public int write( ByteBuffer src, long position ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public MappedByteBuffer map( MapMode mode, long position, long size ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileLock lock( long position, long size, boolean shared ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public FileLock tryLock( long position, long size, boolean shared ) throws IOException
    {
        throw new UnsupportedOperationException();
    }

    @Override
    protected void implCloseChannel() throws IOException
    {
        source.close();
    }
    
    public FileChannel getSource()
    {
        return source;
    }
}