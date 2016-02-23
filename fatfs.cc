/*
 * C++ Version of Adam Dunlap's FAT filesystem
 *
 * Supports writes of arbitrary length to /adam. It also sets the UID and GID of
 * the files to be that of whoever ran the program, so permissions are nice.
 *
 * I'm not sure how access() is different from the mode setting in getattr (mode
 * is just a suggestion, access forces it?), but I had a bug that made it not
 * work and now it does work, so I think I got it.
*/

#define FUSE_USE_VERSION 26

#include <fuse.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <errno.h>

#include <string>
#include <algorithm>
#include <iostream>
#include <fstream>

using std::string;
using std::cerr;
using std::endl;
using std::cout;

static const size_t filestore_size = 10*1024*1024;
static const size_t cluster_size = 4*1024;

string adam_file_contents("Hello there\n");
std::fstream filestore;

using fat_entry = uint32_t;
static const size_t fat_entry_size = sizeof(fat_entry);
static const size_t fat_entries = 1 +   (filestore_size - cluster_size - 1)
                                   / (cluster_size + fat_entry_size);
static const size_t fat_size = fat_entries * fat_entry_size;
static const size_t fat_clusters = 1 + (fat_size - 1) / cluster_size;
static const size_t padded_fat_size = fat_clusters * cluster_size;
static const size_t padded_fat_entries = padded_fat_size / fat_entry_size;
static const size_t fat_blockno = 1;
static const size_t fat_entires_per_cluster = cluster_size / fat_entry_size;

// Writes 1 cluster to the filestore from the pointer
template<typename T>
void write_cluster(size_t cluster_num, const T* data) {
  cout << "Writing to cluster " << cluster_num << endl;

  filestore.seekp(cluster_num * cluster_size);
  filestore.write(reinterpret_cast<const char*>(data), cluster_size);
  filestore.flush();
}

// Writes the given vector. The vector may be modified to pad but will be put
// back in the same state it was given in.
//template<typename T>
//void write_vector(size_t cluster_num, std::vector<T>& data) {
//  cout << "Writing vector of size " << data.size() << " to cluster "
//    << cluster_num << endl;
//  size_t orig_size = data.size();
//  size_t padded_size = (1 + (orig_size-1) / cluster_size) * cluster_size;
//
//  cout << "orig size: " << orig_size << " new size: " << padded_size << endl;
//  data.resize(padded_size, 0);
//  for (size_t i=0; i<padded_size / cluster_size; ++i) {
//    write_cluster(cluster_num + i, data.data() + i * cluster_size);
//  }
//  data.resize(orig_size);
//}

// Reads 1 cluster from the filestore into the pointer
template<typename T>
void read_cluster(size_t cluster_num, T* data) {
  cout << "Reading from cluster " << cluster_num << endl;
  filestore.seekg(cluster_num*cluster_size);
  filestore.read(reinterpret_cast<char*>(data), cluster_size);
}

class FAT_t {
  std::vector<fat_entry> FAT;
 public:

  FAT_t() : FAT(padded_fat_entries)
  {
  }

  // Sets a value in the FAT and persists the FAT to disk
  // TODO: should only write the changed block
  void set(size_t idx, fat_entry val) {
    FAT[idx] = val;
    //persist();
    const size_t cluster_idx = idx / cluster_size;
    write_cluster(fat_blockno + cluster_idx,
                  FAT.data() + cluster_idx*fat_entires_per_cluster);
  }

  // Writes the FAT to disk
  //void persist() {
  //  write_vector(fat_blockno, FAT);
  //}

  // Gets a value from the in-memory FAT
  fat_entry get(size_t idx) {
    return FAT[idx];
  }
  
  // Loads the FAT from the filestore file
  void load() {
    for (size_t i=0; i < fat_clusters; ++i) {
      read_cluster(fat_blockno+i, FAT.data() + i*fat_entires_per_cluster);
    }
  }

  // Resets the FAT to all 0's
  void reset() {
    std::fill(FAT.begin(), FAT.end(), 0);
    //persist();
    for(size_t cluster_idx=0; cluster_idx<fat_clusters; ++cluster_idx) {
      write_cluster(fat_blockno + cluster_idx,
                    FAT.data() + cluster_idx*fat_entires_per_cluster);
    }
  }
} FAT;

static int adam_getattr(const char *cpath, struct stat *stbuf)
{
  string path(cpath);

  cerr << "ADAM: adam_getattr called on ``" << path << "''" << endl;

  memset(stbuf, 0, sizeof(struct stat));

  stbuf->st_dev = 2; // TODO
  stbuf->st_ino = 3; // TODO
  if (path == "/") {
    stbuf->st_mode = S_IFDIR | 0755;
    stbuf->st_nlink = 2;
    stbuf->st_size = 0;
  } else if (path == "/adam") {
    stbuf->st_mode = S_IFREG | 0644;
    stbuf->st_nlink = 1;
    stbuf->st_size = adam_file_contents.length();
  } else {
    return -ENOENT;
  }
  stbuf->st_uid = getuid();
  stbuf->st_gid = getgid();
  stbuf->st_rdev = 0;

  stbuf->st_blksize = 512;
  stbuf->st_blocks = 3;
  struct timespec time0 = {1455428262, 0};
  stbuf->st_atim = time0;
  stbuf->st_mtim = time0;
  stbuf->st_ctim = time0;

  return 0;
}

static int adam_access(const char *cpath, int mask)
{
  string path(cpath);
  if (path == "/") {
      return 0;
  } else if (path == "/adam") {
    if (mask & X_OK) {
      return -1;
    } else {
      return 0;
    }
  } else {
    return -1;
  }
}

static int adam_readlink(const char *path, char *buf, size_t size)
{
  fprintf(stderr, "ADAM: adam_readlink not implemented\n");
  return -ENOSYS;
}


static int adam_readdir(const char *cpath, void *buf, fuse_fill_dir_t filler,
                       off_t offset, struct fuse_file_info *fi)
{
  string path(cpath);
  cerr << "ADAM: adam_readdir called on ``" << path << "'' with offset " << offset << endl;

  if (path == "/") {
    filler(buf, ".", nullptr, 0);
    filler(buf, "..", nullptr, 0);
    filler(buf, "adam", nullptr, 0);
  } else {
    return -ENOENT;
  }
  return 0;

//        DIR *dp;
//        struct dirent *de;
//
//        (void) offset;
//        (void) fi;
//
//        dp = opendir(path);
//        if (dp == NULL)
//                return -errno;
//
//        while ((de = readdir(dp)) != NULL) {
//                struct stat st;
//                memset(&st, 0, sizeof(st));
//                st.st_ino = de->d_ino;
//                st.st_mode = de->d_type << 12;
//                if (filler(buf, de->d_name, &st, 0))
//                        break;
//        }
//
//        closedir(dp);
//        return 0;
}

static int adam_mknod(const char *path, mode_t mode, dev_t rdev)
{
  fprintf(stderr, "ADAM: adam_mknod not implemented\n");
  return -ENOSYS;
}

static int adam_mkdir(const char *path, mode_t mode)
{
  fprintf(stderr, "ADAM: adam_mkdir not implemented\n");
  return -ENOSYS;
}

static int adam_unlink(const char *path)
{
  fprintf(stderr, "ADAM: adam_unlink not implemented\n");
  return -ENOSYS;
}

static int adam_rmdir(const char *path)
{
  fprintf(stderr, "ADAM: adam_rmdir not implemented\n");
  return -ENOSYS;
}

static int adam_symlink(const char *to, const char *from)
{
  fprintf(stderr, "ADAM: adam_symlink not implemented\n");
  return -ENOSYS;
}

static int adam_rename(const char *from, const char *to)
{
  fprintf(stderr, "ADAM: adam_rename not implemented\n");
  return -ENOSYS;
}

static int adam_link(const char *from, const char *to)
{
  fprintf(stderr, "ADAM: adam_link not implemented\n");
  return -ENOSYS;
}

static int adam_chmod(const char *path, mode_t mode)
{
  fprintf(stderr, "ADAM: adam_chmod not implemented\n");
  return -ENOSYS;
}

static int adam_chown(const char *path, uid_t uid, gid_t gid)
{
  fprintf(stderr, "ADAM: adam_chown not implemented\n");
  return -ENOSYS;
}

static int adam_truncate(const char *cpath, off_t size)
{
  string path(cpath);
  if (path == "/adam") {
    adam_file_contents.resize(size);
    return 0;
  } else {
    return -ENOENT;
  }
}

static int adam_utimens(const char *path, const struct timespec ts[2])
{
  fprintf(stderr, "ADAM: adam_utimens not implemented\n");
  return -ENOSYS;
}

static int adam_open(const char *cpath, struct fuse_file_info *fi)
{
  string path(cpath);
  if (path == "/" || path == "/adam") {
    return 0;
  }
  return -ENOENT;
}

static int adam_read(const char *cpath, char *buf, size_t size, off_t offset,
                    struct fuse_file_info *fi)
{

  string path(cpath);
  if (path == "/adam") {
    auto offset_contents = adam_file_contents.begin();
    for(off_t i=0; i<offset && offset_contents != adam_file_contents.end();
        ++i, ++offset_contents);

    size_t rest_of_length = std::distance(offset_contents,
                                          adam_file_contents.end());
    size_t num_to_return = std::min(size, rest_of_length);

    std::copy_n(offset_contents, num_to_return, buf);

    return num_to_return;
  } else {
    return -ENOENT;
  }
        int fd;
        int res;

        (void) fi;
        fd = open(cpath, O_RDONLY);
        if (fd == -1)
                return -errno;

        res = pread(fd, buf, size, offset);
        if (res == -1)
                res = -errno;

        close(fd);
        return res;
}

static int adam_write(const char *cpath, const char *buf, size_t size,
                     off_t offset, struct fuse_file_info *fi)
{
  string path(cpath);
  if (path == "/adam") {
    const size_t new_file_size =
      std::max(adam_file_contents.length(), size+offset);
    adam_file_contents.resize(new_file_size);
    auto offset_file_contents_it =
      std::next(adam_file_contents.begin(), offset);
    std::copy_n(buf, size, offset_file_contents_it);
    return size;
  } else {
    return -1;
  }

        int fd;
        int res;

        (void) fi;
        fd = open(cpath, O_WRONLY);
        if (fd == -1)
                return -errno;

        res = pwrite(fd, buf, size, offset);
        if (res == -1)
                res = -errno;

        close(fd);
        return res;
}

static int adam_statfs(const char *path, struct statvfs *stbuf)
{
  fprintf(stderr, "ADAM: adam_statfs not implemented\n");
  return -ENOSYS;
}

static int adam_release(const char *path, struct fuse_file_info *fi)
{
  fprintf(stderr, "ADAM: adam_release not implemented\n");
  return -ENOSYS;
}

static int adam_fsync(const char *path, int isdatasync,
                     struct fuse_file_info *fi)
{
  fprintf(stderr, "ADAM: adam_fsync not implemented\n");
  return -ENOSYS;
}


static struct fuse_operations adam_oper;
int main(int argc, char *argv[])
{
  static_assert(filestore_size > 0, "No room to store backend");
  static_assert(cluster_size > 0, "0 size cluster");
  static_assert(padded_fat_entries * fat_entry_size == padded_fat_size,
      "cluster size not multiple of fat entry size");

  if (argc < 1) {
    cerr << "First argument should be name of file" << endl;
    return 2;
  }
  string filestorename(argv[1]);
  --argc;
  ++argv;
  
  cout << "Opening " << filestorename << " as FAT backend." << endl;
  filestore.open(filestorename, std::ios_base::binary);

  if (!filestore.good()) {
    cout << "File did not exist, initializing file with " << filestore_size <<
      " bytes" << endl;
    filestore.open(filestorename, std::ios_base::out | std::ios_base::binary);

    // Seek to end and write a 0 byte
    // Probably not necessary...
    //filestore.seekp(filestore_size-1);
    //filestore.write("", 1);

    FAT.reset();

    //FAT.set(0, 'a');
    //FAT.set(1, 'b');
    //FAT.set(2, 'c');
    //FAT.set(8, 'd');

    //FAT.set(fat_size-1, 382);
    //FAT.set(fat_size-3, 292);
    //FAT.set(fat_size-3, 47);
    //FAT.set(fat_entries-3, 'e');
  } else {
    FAT.load();
  }

  //cout << "FAT entries " << fat_entries << endl;
  //cout << "FAT padded entries " << padded_fat_entries << endl;
  //cout << "FAT size " << fat_size << endl;
  //cout << "FAT padded size " << padded_fat_size << endl;

  //for (int i=0; i<10; ++i) {
  //  cout << FAT.get(i) << endl;
  //}
  //cout << "..." << endl;
  //for (int i=10; i>0; --i) {
  //  cout << FAT.get(fat_entries-i) << endl;
  //}

  //std::vector<char> writebuf(cluster_size);
  //writebuf[42] = 'x';
  //writebuf[43] = 'y';
  //writebuf[44] = 'z';
  //writebuf[45] = 'm';
  //write_cluster(201, writebuf.data());

  ////filestore.close();
  ////filestore.open(filestorename, std::ios_base::binary);

  //std::vector<char> readbuf(cluster_size);
  //read_cluster(201, readbuf.data());
  //cout << readbuf[42] << endl;
  //cout << readbuf[43] << endl;
  //cout << readbuf[44] << endl;
  //cout << readbuf[45] << endl;

  adam_oper.getattr   = adam_getattr;
  adam_oper.access    = adam_access;
  adam_oper.readlink  = adam_readlink;
  adam_oper.readdir   = adam_readdir;
  adam_oper.mknod     = adam_mknod;
  adam_oper.mkdir     = adam_mkdir;
  adam_oper.symlink   = adam_symlink;
  adam_oper.unlink    = adam_unlink;
  adam_oper.rmdir     = adam_rmdir;
  adam_oper.rename    = adam_rename;
  adam_oper.link      = adam_link;
  adam_oper.chmod     = adam_chmod;
  adam_oper.chown     = adam_chown;
  adam_oper.truncate  = adam_truncate;
  adam_oper.utimens   = adam_utimens;
  adam_oper.open      = adam_open;
  adam_oper.read      = adam_read;
  adam_oper.write     = adam_write;
  adam_oper.statfs    = adam_statfs;
  adam_oper.release   = adam_release;
  adam_oper.fsync     = adam_fsync;
  adam_oper.flag_nullpath_ok = 0;

  umask(0);
  return fuse_main(argc, argv, &adam_oper, NULL);
}
