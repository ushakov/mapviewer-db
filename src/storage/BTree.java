package storage;

import org.apache.log4j.Logger;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.util.LinkedList;

public class BTree {
    private static final Logger log = Logger.getLogger(BTree.class);
    private RandomAccessFile idxfile;
    private RandomAccessFile datafile;

    private TileCache<Integer, BTreeNode> cache;
    private BTreeNode root;

    public BTree() {
        root = null;
        cache = new TileCache<Integer, BTreeNode>(2000000 / BTreeNode.NODE_SIZE);
    }

    public boolean open(String base) {
        try {
            cache.clear();
            idxfile = new RandomAccessFile(base + ".idx", "rw");
            datafile = new RandomAccessFile(base + ".dat", "rw");
            root = getNode(0);
            if (root == null) {
                root = new BTreeNode();
                root.setIsLeaf(true);
                root.writeTo(this, 0);
                cache.put(0, root);
            }
            return true;
        } catch (FileNotFoundException e) {
            idxfile = null;
            datafile = null;
            return false;
        }
    }

    public boolean close() {
        try {
            idxfile.close();
            datafile.close();
            return true;
        } catch (IOException e) {
            return false;
        }
    }

    public RandomAccessFile getIdxfile() {
        return idxfile;
    }

    private byte[] getContent(long link) {
        try {
            datafile.seek(link);
            int len = 0;
            int mult = 1;
            for (int i = 0; i < 4; ++i) {
                len += mult * datafile.readByte();
                mult <<= 8;
            }
            byte[] buffer = new byte[len];
            datafile.read(buffer);
            return buffer;
        } catch (IOException e) {
            return null;
        }
    }

    private long appendContent(byte[] content) {
        try {
            long offset = datafile.length();
            datafile.seek(offset);
            int len = content.length;
            for (int i = 0; i < 4; ++i) {
                datafile.writeByte(len & 0xff);
                len >>= 8;
            }
            datafile.write(content);
            return offset;
        } catch (IOException e) {
            return -1;
        }
    }

    private BTreeNode getNode(int addr) {
        if (cache.hasKey(addr)) {
                return cache.get(addr);
        }
        BTreeNode node = new BTreeNode();
        if (!node.fetch(this, addr)) {
            return null;
        }
        cache.put(addr, node);
        return node;
    }

    private int nextAddr() {
        try {
            long idxlen = idxfile.length();
            if ((idxlen & (BTreeNode.NODE_SIZE - 1)) != 0) {
                idxlen = (idxlen & ~(BTreeNode.NODE_SIZE - 1)) + BTreeNode.NODE_SIZE;
            }
            return (int)(idxlen / BTreeNode.NODE_SIZE);
        } catch (IOException e) {
            return -1;
        }

    }

    public byte[] getFirst(int key) {
        BTreeNode current = root;
//        log.debug("getFirst: current=" + current.getAddr());
        while (!current.isLeaf()) {
            int idx = current.search(key);
            int link = (int)current.getLink(idx);
            current = getNode(link);
//            log.debug("getFirst: current=" + current.getAddr());
        }
        int idx = current.search(key);
        if (current.getKey(idx) == key) {
            long offset = current.getLink(idx);
            return getContent(offset);
        } else {
            return null;
        }
    }

    public void insert(int key, byte[] content) {
        BTreeNode current = root;
//        log.debug("insert: current = " + current.getAddr());
        LinkedList<BTreeNode> parents = new LinkedList<BTreeNode>();
        while (!current.isLeaf()) {
            int idx = current.search(key);
            int link = (int)current.getLink(idx);
//            log.debug("search gives: idx=" + idx + " link=" + link);
            parents.add(current);
            current = getNode(link);
//            log.debug("insert: current = " + current.getAddr());
        }
        if (current.size() < BTreeNode.HIGH_MARK) {
            long link = appendContent(content);
            current.insert(key, link);
            current.store();
//            log.debug("inserted to current, link=" + link);
            return;
        }
        int to_insert_key = key;
        long to_insert_link = appendContent(content);
        while (current != null && current.size() == BTreeNode.HIGH_MARK) {
            // Node current is full. Split it and insert the needed key, then
            // leave the insertion of pivot into parent for the next iteration.
//            log.debug("splitting " + current.getAddr());
            BTreeNode sibling = new BTreeNode();
            sibling.setIsLeaf(current.isLeaf());
            int pivotKey = current.getKey(BTreeNode.LOW_MARK - 1);
            current.split(BTreeNode.LOW_MARK, sibling);
            if (to_insert_key <= pivotKey) {
                sibling.insert(to_insert_key, to_insert_link);
            } else {
                current.insert(to_insert_key, to_insert_link);
            }
            sibling.writeTo(this, nextAddr());
            current.store();
            current = parents.pollLast();
            to_insert_key = pivotKey;
            to_insert_link = sibling.getAddr();
//            log.debug("new sibling=" + sibling.getAddr() + ", will insert " + to_insert_key + "->" + to_insert_link);
        }
        if (current != null) {
            // We have found a non-full node uptree.
            current.insert(to_insert_key, to_insert_link);
            current.store();
//            log.debug("finished by inserting " + to_insert_key + "->" + to_insert_link + " into " + current.getAddr());
        } else {
            // We have just split the old root. Left is to_insert_link, right is root.
            // Root is always at address 0, so we have to shuffle nodes a bit, which spoils cache.
            // We thus tweak the cache afterwards.
            root.writeTo(this, nextAddr());
            cache.put(root.getAddr(), root);
            BTreeNode newRoot = new BTreeNode();
            newRoot.setIsLeaf(false);
            newRoot.insert(to_insert_key, to_insert_link);
//            log.debug("setlink in new root: " + newRoot.size() + "->" + root.getAddr());
            newRoot.setLink(newRoot.size(), root.getAddr());
            newRoot.writeTo(this, 0);
            cache.put(newRoot.getAddr(), newRoot);
//            log.debug("old root goes to " + root.getAddr());
            root = newRoot;

        }
    }

//    public void dump() {
//        log.debug("Dumping tree");
//        LinkedList<BTreeNode> level = new LinkedList<BTreeNode>();
//        level.add(getNode(0));
//        while (level.size() > 0) {
//            LinkedList<BTreeNode> newlevel = new LinkedList<BTreeNode>();
//            for (BTreeNode node : level) {
//                node.dump();
//                if (!node.isLeaf()) {
//                    for (int i = 0; i < node.size() + 1; ++i) {
//                        newlevel.add(getNode((int)node.getLink(i)));
//                    }
//                }
//            }
//            level = newlevel;
//        }
//    }

    public boolean invariantsPass() {
        LinkedList<BTreeNode> level = new LinkedList<BTreeNode>();
        if (root != getNode(0)) {
            log.debug("root is not getNode(0)");
            return false;
        }
        level.add(root);
        while (level.size() > 0) {
            LinkedList<BTreeNode> newlevel = new LinkedList<BTreeNode>();
            boolean leafLevel = false;
            boolean leafnessKnown = false;
            for (BTreeNode node : level) {
                if (!node.invariantsPass()) {
                    log.debug("node " + node.getAddr() + " failed invariant testing");
                    return false;
                }
                if (!node.isLeaf()) {
                    for (int i = 0; i < node.size() + 1; ++i) {
                        newlevel.add(getNode((int)node.getLink(i)));
                    }
                }
                if (!leafnessKnown) {
                    leafnessKnown = true;
                    leafLevel = node.isLeaf();
                } else {
                    if (leafLevel != node.isLeaf()) {
                        log.debug("inconsistent leafness at node " + node.getAddr());
                        return false;
                    }
                }
            }
            level = newlevel;
        }
        return true;
    }
}
