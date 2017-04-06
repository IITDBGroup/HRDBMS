package cs597;

import java.io.*;
import java.nio.ByteBuffer;

public class HdfsRead3 {
    public static void main(String[] args) throws IOException {
//
//        Configuration conf = new Configuration();
//        conf.addResource(new Path("core-site.xml"));
//        conf.addResource(new Path("hdfs-site.xml"));
//
//        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
//        System.out.println("Enter the file path...");
//        String filePath = "hdfs://17.17.0.5:9000/user/root/input/log4j.properties";
//
//        Path path = new Path(filePath);
//        FileSystem fs = path.getFileSystem(conf);
//        FSDataInputStream inputStream = fs.open(path);
//        System.out.println(inputStream.available());
//        fs.close();

        ByteBuffer b = ByteBuffer.wrap("abcdefghij".getBytes());
        BufferedReader r = wrapByteBuffer(b);
        System.out.println(r.readLine());
    }

    private static BufferedReader wrapByteArray(byte[] byteArr) {
        return wrapByteArray(byteArr, 0, byteArr.length);
    }
    private static BufferedReader wrapByteArray(byte[] byteArr, int offset, int length) {
        ByteArrayInputStream stream = new ByteArrayInputStream(byteArr, offset, length);
        InputStreamReader sr = new InputStreamReader(stream);
        BufferedReader reader = new BufferedReader(sr);
        return reader;
    }
    private static BufferedReader wrapByteBuffer(ByteBuffer byteBuffer) {
        return wrapByteArray(byteBuffer.array());
    }

}
