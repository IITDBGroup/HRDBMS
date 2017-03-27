package cs597;

import org.apache.commons.vfs2.*;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

/**
 * Created by felix on 3/26/17.
 */

public class Main {
    public static void main(String[] args) throws IOException {
        FileSystemManager fsManager = VFS.getManager();

        FileObject blar = fsManager.resolveFile("hdfs://172.17.0.2:9000/user/root/input/log4j.properties");
        FileContent content = blar.getContent();
        BufferedReader reader = new BufferedReader(new InputStreamReader(content.getInputStream()));
        System.out.println("readline " + reader.readLine());
        reader.close();
    }
}
