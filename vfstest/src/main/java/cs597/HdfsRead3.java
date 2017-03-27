package cs597;
import org.apache.hadoop.conf.Configuration;
        import org.apache.hadoop.fs.FSDataInputStream;
        import org.apache.hadoop.fs.FileSystem;
        import org.apache.hadoop.fs.Path;

        import java.io.BufferedReader;
        import java.io.IOException;
        import java.io.InputStreamReader;

public class HdfsRead3 {
    public static void main(String[] args) throws IOException {

        Configuration conf = new Configuration();
        conf.addResource(new Path("core-site.xml"));
        conf.addResource(new Path("hdfs-site.xml"));

        BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
        System.out.println("Enter the file path...");
        String filePath = "hdfs://17.17.0.5:9000/user/root/input/log4j.properties";

        Path path = new Path(filePath);
        FileSystem fs = path.getFileSystem(conf);
        FSDataInputStream inputStream = fs.open(path);
        System.out.println(inputStream.available());
        fs.close();
    }
}