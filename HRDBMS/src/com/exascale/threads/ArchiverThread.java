package com.exascale.threads;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.util.HashSet;
import java.util.List;

import com.exascale.logging.LogRec;
import com.exascale.logging.NQCheckLogRec;
import com.exascale.managers.HRDBMSWorker;
import com.exascale.managers.LogManager;

public class ArchiverThread extends HRDBMSThread
{
	protected String filename;
	
	public ArchiverThread(String filename)
	{
		this.filename = filename;
		this.setWait(false);
		this.description = "Archiver Thread";
	}
	
	public void run()
	{
		synchronized(LogManager.noArchive)
		{
			if (!LogManager.noArchive)
			{
				FileChannel fc = null;
				try
				{
					fc = LogManager.getFile(filename);
				}
				catch(IOException e)
				{
					HRDBMSWorker.logger.error("Archiving error while getting FileChannel for " + filename, e);
					this.terminate();
					return;
				}
				synchronized(fc)
				{
					try
					{
						fc.position(fc.size() - 4);
						ByteBuffer sizeBuff = ByteBuffer.allocate(4);
			
						while (true)
						{
							sizeBuff.position(0);
							fc.read(sizeBuff);
							sizeBuff.position(0);
							int size = sizeBuff.getInt();
							fc.position(fc.position() -4 - size);
							LogRec rec = new LogRec(fc);
			
							if (rec.type() == LogRec.NQCHECK)
							{
								NQCheckLogRec r = (NQCheckLogRec)rec.rebuild();
								HashSet<Long> list = r.getOpenTxs();
					
								//find start of oldest tx
								fc.position(fc.position() - size - 8);
								while (true)
								{
									sizeBuff.position(0);
									fc.read(sizeBuff);
									sizeBuff.position(0);
									size = sizeBuff.getInt();
									fc.position(fc.position() -4 - size);
									rec = new LogRec(fc);
					
									if (rec.type() == LogRec.START)
									{
										list.remove(rec.txnum());
						
										if (list.isEmpty())
										{
											fc.position(fc.position() - size - 4);
											long end = fc.position();
								
											fc.position(4);
											LogRec first = new LogRec(fc);
											long startLSN = first.lsn();
							
											String archiveDir = HRDBMSWorker.getHParms().getProperty("archive_dir");
											if (!archiveDir.endsWith("/"))
											{
												archiveDir += "/";
											}
							
											String archive = archiveDir + filename + "_startLSN_" + startLSN + ".log";
											File archiveLog = new File(archive);
											File activeLog = new File(filename);
											
											activeLog.renameTo(archiveLog);
											activeLog.createNewFile();
											FileChannel newActive = new RandomAccessFile(activeLog, "rws").getChannel();
											FileChannel newArchive = new RandomAccessFile(archiveLog, "rws").getChannel();
											newArchive.transferTo(end, newArchive.size() - end, newActive);
											LogManager.openFiles.put(filename, newActive);
											
											newArchive.truncate(end);
											newArchive.close();
											this.terminate();
											return;
										}	
									}
									else
									{
										try
										{
											fc.position(fc.position() - size - 8);
										}
										catch(IllegalArgumentException e)
										{
											this.terminate();
											return;
										}
									}
								}
							}
							else
							{
								try
								{
									fc.position(fc.position() - size - 8);
								}
								catch(IllegalArgumentException e)
								{
									this.terminate();
									return;
								}		
							}
						}	
					}
					catch(IOException e)
					{
						HRDBMSWorker.logger.error("Error occurred during archiving.", e);
						this.terminate();
						return;
					}
				}
			}
		}
		
		this.terminate();
		return;
	}
}
