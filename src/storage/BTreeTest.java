package storage;

import junit.framework.TestCase;
import org.apache.log4j.BasicConfigurator;

import java.io.File;

public class BTreeTest extends TestCase {
//    private static final Logger log = Logger.getLogger(BTreeTest.class);

    static {
        BasicConfigurator.configure();
    }

    private BTree tree;

    public void setUp() {
        tree = new BTree();
        File dbfile = new File("test.idx");
        dbfile.delete();
        dbfile = new File("test.dat");
        dbfile.delete();
        assertTrue(tree.open("test"));
    }

    public void tearDown() {
        assertTrue(tree.close());
    }

    public void testInsert() {
        byte[] buf = new byte[12];
        buf[0] = 57;
        buf[1] = 43;
        tree.insert(33, buf);
        byte[] read = tree.getFirst(33);
        assertNotNull(read);
        assertEquals(12, read.length);
        assertEquals(57, read[0]);
        assertEquals(43, read[1]);
    }

    public void testInsertMany() {
        byte[] buf = new byte[12];
        buf[0] = 57;
        buf[1] = 43;
        for(int i = 0; i < 1000; ++i) {
            buf[2] = (byte)(i & 0xff);
            buf[3] = (byte)(i >> 8);
            tree.insert(i, buf);
            assertTrue(tree.invariantsPass());
        }
        for (int i = 999; i >=0; --i) {
            byte[] read = tree.getFirst(i);
            assertNotNull(read);
            assertEquals(57, read[0]);
            assertEquals((byte)(i%256), read[2]);
            assertEquals((byte)(i/256), read[3]);
        }
    }
}
