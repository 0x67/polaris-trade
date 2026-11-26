/* C host for tests/c_host.rs. Opens a feed session through the C ABI, polls it
 * to the end a few records at a time, and writes every record to a file: its
 * stream as 2 bytes and its length as 4, both little-endian, then its bytes.
 *
 * Usage: harness <config json> <output path>
 * Exit status: 0 when the poll loop ends on POLARIS_FEED_END and close
 * returns POLARIS_OK, 1 otherwise, 2 on a usage error. */
#include <stdio.h>

#include "polaris_abi.h"

#if POLARIS_ABI_VERSION != 1
#error "harness.c is written for ABI version 1"
#endif

/* smaller than one frame batch, so records carry over between polls */
#define CAP 7

static int write_le(FILE *out, uint32_t value, size_t width) {
    unsigned char buf[4];
    size_t i;
    for (i = 0; i < width; i++) {
        buf[i] = (unsigned char)(value >> (8 * i));
    }
    return fwrite(buf, 1, width, out) == width;
}

static int write_record(FILE *out, const polaris_record *rec) {
    return write_le(out, rec->stream, 2) && write_le(out, rec->len, 4) &&
           fwrite(rec->bytes, 1, rec->len, out) == rec->len;
}

int main(int argc, char **argv) {
    polaris_feed *feed = NULL;
    polaris_record records[CAP];
    size_t written = 0;
    size_t i;
    int32_t polled;
    int32_t closed;
    int write_failed = 0;
    FILE *out;

    if (argc != 3) {
        fprintf(stderr, "usage: harness <config json> <output path>\n");
        return 2;
    }
    out = fopen(argv[2], "wb");
    if (out == NULL) {
        fprintf(stderr, "cannot open %s\n", argv[2]);
        return 1;
    }
    polled = polaris_feed_open(argv[1], &feed);
    if (polled != POLARIS_OK) {
        fprintf(stderr, "polaris_feed_open: %d\n", (int)polled);
        fclose(out);
        return 1;
    }

    while ((polled = polaris_feed_poll(feed, records, CAP, &written)) == POLARIS_OK) {
        for (i = 0; i < written && !write_failed; i++) {
            write_failed = !write_record(out, &records[i]);
        }
        if (write_failed) {
            break;
        }
    }
    closed = polaris_feed_close(feed);

    if (fclose(out) != 0 || write_failed) {
        fprintf(stderr, "writing %s failed\n", argv[2]);
        return 1;
    }
    if (polled != POLARIS_FEED_END) {
        fprintf(stderr, "polaris_feed_poll: %d\n", (int)polled);
        return 1;
    }
    if (closed != POLARIS_OK) {
        fprintf(stderr, "polaris_feed_close: %d\n", (int)closed);
        return 1;
    }
    return 0;
}
