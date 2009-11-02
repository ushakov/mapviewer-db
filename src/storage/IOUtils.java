package storage;

public class IOUtils {
  // Currently Dalvik VM limits the whole application to 16 MB.
  private static final int MAX_STRING_LEN = 32 * 1024 * 1024;

  public static int readIntBE(byte[] buffer, int offset) {
    int b1 = buffer[offset] & 0xff; offset++;
    int b2 = buffer[offset] & 0xff; offset++;
    int b3 = buffer[offset] & 0xff; offset++;
    int b4 = buffer[offset] & 0xff;
    return ((b1 << 24) + (b2 << 16) + (b3 << 8) + b4);
  }

  public static int readIntLE(byte[] buffer, int offset) {
    int b1 = buffer[offset] & 0xff; offset++;
    int b2 = buffer[offset] & 0xff; offset++;
    int b3 = buffer[offset] & 0xff; offset++;
    int b4 = buffer[offset] & 0xff;
    return ((b4 << 24) + (b3 << 16) + (b2 << 8) + b1);
  }

  public static void readIntArrayBE(byte[] buffer, int offset, int[] dst, int size) {
    for (int i = 0; i < size; i++) {
      int b1 = buffer[offset] & 0xff; offset++;
      int b2 = buffer[offset] & 0xff; offset++;
      int b3 = buffer[offset] & 0xff; offset++;
      int b4 = buffer[offset] & 0xff; offset++;
      dst[i] = ((b1 << 24) + (b2 << 16) + (b3 << 8) + b4);
    }
  }

  public static void writeIntArrayBE(int[] src, int start, int count,
      byte[] dst, int offset) {
    for (int i = 0; i < count; i++) {
      int word = src[start + i];
      byte b4 = (byte) (word & 0xff); word >>= 8;
      byte b3 = (byte) (word & 0xff); word >>= 8;
      byte b2 = (byte) (word & 0xff); word >>= 8;
      byte b1 = (byte) (word & 0xff);
      dst[offset++] = b1;
      dst[offset++] = b2;
      dst[offset++] = b3;
      dst[offset++] = b4;
    }
  }
}
