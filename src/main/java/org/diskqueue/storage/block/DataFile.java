package org.diskqueue.storage.block;

import org.diskqueue.storage.manager.FileManager;
import org.diskqueue.utils.RefCount;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;

/**
 * Files which contain BlockHeader and Blocks
 */
public class DataFile extends RefCount {
    public static final int DATA_FILE_LENGTH = 512 * 1024 * 1024;

    // belonged FileManager. used to recyle itself
    private FileManager fileManager;
    // Java File container
    private File mmappedFile;

    // header of the file
    private DataFileHeader header;
    // block list
    private Block presentBlock;

    // memory map related
    private FileChannel mmapFileChannel;
    private MappedByteBuffer mmapBuffer;

    public DataFile(FileManager fileManager, String fileName) {
        this.fileManager = fileManager;
        this.mmappedFile = new File(fileName);
    }

    public void load() throws IllegalAccessException {
        try {
            if (mmappedFile.exists() && mmappedFile.canRead() && mmappedFile.canWrite()) {
                // make channel
                mmapFileChannel = new RandomAccessFile(mmappedFile, "rw").getChannel();
                // mount entire file to mmap byte array buffer
                mmapBuffer = mmapFileChannel.map(FileChannel.MapMode.READ_WRITE, 0, mmappedFile.length());

                // parse datafile header
//                mmapBuffer.order()

            } else
                throw new IllegalAccessException("mmap file access failed " + mmappedFile.getAbsolutePath());
        } catch (IOException e) {
            throw new IllegalAccessException("file not found " + mmappedFile.getAbsolutePath());
        }
    }

    @Override
    public void recyle() {
        fileManager.putback(this);
    }

    public void flushAll() {

    }

    public File getGenericFile() {
        return mmappedFile;
    }
}
