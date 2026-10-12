/* A mount/WRITE/READ/FSYNC support probe, not a passthrough filesystem.
 * Based on the documented libfuse3 high-level callback signatures.
 */
#define FUSE_USE_VERSION 31
#include <fuse3/fuse.h>
#include <errno.h>
#include <stdio.h>
#include <string.h>
#include <sys/stat.h>

static char bytes[4096];
static size_t length;
static int attr(const char *p, struct stat *s, struct fuse_file_info *fi) {
    (void)fi;
    memset(s, 0, sizeof(*s));
    if (!strcmp(p, "/")) { s->st_mode = S_IFDIR | 0700; s->st_nlink = 2; return 0; }
    if (strcmp(p, "/probe")) return -ENOENT;
    s->st_mode = S_IFREG | 0600; s->st_nlink = 1; s->st_size = length; return 0;
}
static int open_file(const char *p, struct fuse_file_info *fi) {
    if (strcmp(p, "/probe")) return -ENOENT;
    fi->direct_io = 1;
    return 0;
}
static int write_file(const char *p, const char *b, size_t n, off_t o, struct fuse_file_info *fi) {
    (void)p; (void)fi;
    if (o < 0 || (size_t)o > sizeof(bytes) || n > sizeof(bytes) - (size_t)o) return -ENOSPC;
    memcpy(bytes + o, b, n);
    if ((size_t)o + n > length) length = (size_t)o + n;
    fprintf(stderr, "FIRED write n=%zu offset=%lld\n", n, (long long)o);
    return (int)n;
}
static int read_file(const char *p, char *b, size_t n, off_t o, struct fuse_file_info *fi) {
    (void)p; (void)fi;
    if (o < 0) return -EINVAL;
    if ((size_t)o >= length) return 0;
    if (n > length - (size_t)o) n = length - (size_t)o;
    memcpy(b, bytes + o, n); return (int)n;
}
static int sync_file(const char *p, int datasync, struct fuse_file_info *fi) {
    (void)p; (void)datasync; (void)fi;
    fprintf(stderr, "FIRED fsync errno=EIO\n");
    return -EIO;
}
int main(int argc, char **argv) {
    struct fuse_operations ops = {
        .getattr = attr, .open = open_file, .read = read_file,
        .write = write_file, .fsync = sync_file
    };
    return fuse_main(argc, argv, &ops, NULL);
}
