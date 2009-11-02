package storage;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.io.RandomAccessFile;


// Block layout (values are big-endian):
// offset   description
// 0        leaf -> 1, non-leaf: 0
// 1..2     number of keys stored
// 3..402   keys of this block, 4 bytes per key
// 403..907 links of this block, 5 bytes per link
public class BTreeNode {
    private static final Logger log = Logger.getLogger(BTreeNode.class);
    public static final int NODE_SIZE = 1024;
    public static final int LINK_SIZE = 505;
    public static final int LINK_START = 403;

    public static final int LOW_MARK = 50;
    public static final int HIGH_MARK = 100;

    private int[] keys;
    private byte[] links;
    private boolean isLeaf;
    private int addr;
    private BTree ctx;

    public BTreeNode() {
        ctx = null;
        links = new byte[LINK_SIZE];
        keys = new int[0];
        isLeaf = true;
    }

    public boolean fetch(BTree ctx, int addr) {
        try {
            this.ctx = null;
            RandomAccessFile file = ctx.getIdxfile();
            file.seek(addr * NODE_SIZE);
            isLeaf = file.readByte() == 1;
            byte b1 = file.readByte();
            byte b2 = file.readByte();
            int len = b1 + b2 * 256;
            keys = new int[len];
            byte[] buffer = new byte[len*4];
            file.read(buffer);
            IOUtils.readIntArrayBE(buffer, 0, keys, len);
            file.seek(addr * NODE_SIZE + LINK_START);
            file.read(links);

            this.addr = addr;
            this.ctx = ctx;
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public boolean store() {
        try {
            RandomAccessFile file = ctx.getIdxfile();
            file.seek(addr * NODE_SIZE);
            if (isLeaf) {
                file.writeByte(1);
            } else {
                file.writeByte(0);
            }
            final int len = keys.length;
            file.writeByte(len % 256);
            file.writeByte(len / 256);
            byte[] buffer = new byte[4 * len];
            IOUtils.writeIntArrayBE(keys, 0, len, buffer, 0);
            file.write(buffer);
            file.seek(addr * NODE_SIZE + LINK_START);
            file.write(links);
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public boolean writeTo(BTree ctx, int addr) {
        this.ctx = ctx;
        this.addr = addr;
        return store();
    }


    // Returns index into keys array of the leftmost element not less than key,
    // or keys.length if not found.
    public int search(int key) {
        int low = 0;
        int high = keys.length - 1;
        if (keys[low] >= key) return low;
        if (keys[high] < key) return keys.length;
        // inv.: keys[low] < key && keys[high] >= key; low < high.
        while (high - low > 1) {
            int middle = (low + high)/2;
            if (keys[middle] < key) {
                low = middle;
            } else {
                high = middle;
            }
        }
        return high;
    }

    public long getLink(int index) {
        long link = 0;
        int offset = index * 5 + 4;
        for (int i = 0; i < 5; ++i) {
            link <<= 8;
            link += (((int)links[offset]) + 256) % 256;
            offset--;
        }
        return link;
    }

    public void setLink(int index, long link) {
        int offset = index * 5;
        for (int i = 0; i < 5; ++i) {
            links[offset] = (byte)(link % 256);
            link >>= 8;
            offset ++;
        }
    }

    // Split the node. Left part goes to other, right part remains here.
    // Argument leftPart is the size of left part.
    public void split(int leftPart, BTreeNode other) {
        other.keys = new int[leftPart];
        for(int i = 0; i < leftPart; ++i) {
            other.setLink(i, getLink(i));
            other.keys[i] = keys[i];
        }
        // Shift keys and links in itself
        int[] newKeys = new int[keys.length - leftPart];
        System.arraycopy(keys, leftPart, newKeys, 0, keys.length - leftPart);
        System.arraycopy(links, leftPart*5, links, 0, (keys.length - leftPart + 1) * 5);
        keys = newKeys;
    }

    // Insert a new key. Asserts that there is place for it.
    public void insert(int key, long link) {
        assert keys.length < HIGH_MARK;
        int[] newkeys = new int[keys.length + 1];
        // Copy over the left part.
        int pivotidx = 0;
        for ( ; pivotidx < keys.length && keys[pivotidx] < key; ++pivotidx) {
            newkeys[pivotidx] = keys[pivotidx];
            // links remain the same
        }
        // Copy right part shifting links
        for (int i = keys.length; i >= pivotidx; --i) {
            setLink(i + 1, getLink(i));
            if (i < keys.length) {
                newkeys[i + 1] = keys[i];
            }
        }
        setLink(pivotidx, link);
        newkeys[pivotidx] = key;
        keys = newkeys;
    }

    public boolean isLeaf() {
        return isLeaf;
    }

    public void setIsLeaf(boolean leaf) {
        isLeaf = leaf;
    }

    public int getKey(int idx) {
        return keys[idx];
    }

    public int size() {
        return keys.length;
    }

    public int getAddr() {
        return addr;
    }

//    public void dump() {
//        StringBuilder builder = new StringBuilder();
//        builder.append("Node ").append(addr).append(": [ ");
//        for (int i = 0; i < keys.length; ++i) {
//            builder.append(keys[i]).append("->").append(getLink(i)).append(" ");
//        }
//        builder.append("@->").append(getLink(keys.length));
//        builder.append(" ]");
//        log.debug(builder.toString());
//    }

    public boolean invariantsPass() {
        if (keys.length < LOW_MARK && getAddr() != 0) {
            log.debug("too low key count = " + keys.length);
            return false;
        }
        if (keys.length > HIGH_MARK) {
            log.debug("too high key count = " + keys.length);
            return false;
        }
        for (int i = 0; i < keys.length - 1; ++i) {
            if (keys[i] > keys[i+1]) {
                log.debug("not sorted: key["  + i + "]=" + keys[i] + " > key[" + i+1 + "]=" + keys[i+1]);
                return false;
            }
        }
        return true;
    }
}
