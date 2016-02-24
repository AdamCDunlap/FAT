/*
 * C++ Version of Adam Dunlap's FAT filesystem
 *
 * Block size = cluster size
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
#include <array>

using std::string;
using std::cerr;
using std::endl;
using std::cout;

static const size_t filestore_size = 10*1024*1024;
static const size_t cluster_size = 4*1024;

string adam_file_contents("Hello there\n");
static std::fstream filestore;

using fat_entry = uint32_t;
static const size_t fat_entry_size = sizeof(fat_entry);
static const size_t fat_entries = 1 +   (filestore_size - cluster_size - 1)
                                   / (cluster_size + fat_entry_size);
static const size_t fat_size = fat_entries * fat_entry_size;
static const size_t fat_clusters = 1 + (fat_size - 1) / cluster_size;
static const size_t padded_fat_size = fat_clusters * cluster_size;
static const size_t padded_fat_entries = padded_fat_size / fat_entry_size;
static const size_t fat_entires_per_cluster = cluster_size / fat_entry_size;

static const size_t superblock_cluster = 0;
static const size_t fat_cluster = 1;
// Block at which the root directory starts
static const size_t data_start_block = fat_cluster + fat_clusters;

static const char directory_delim = '/';

// Writes 1 cluster to the filestore from the pointer
template<typename T>
void write_cluster(size_t cluster_num, const T* data) {
  cout << "Writing to cluster " << cluster_num << endl;

  filestore.seekp(cluster_num * cluster_size);
  filestore.write(reinterpret_cast<const char*>(data), cluster_size);
  filestore.flush();
}

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

  // Sets a value in the in-memory FAT. DOES NOT persist
  void weak_set(size_t idx, fat_entry val) {
    FAT[idx] = val;
  }

  // Sets a value in the FAT and persists the change to disk
  void set(size_t idx, fat_entry val) {
    weak_set(idx, val);

    const size_t cluster_idx = idx / cluster_size;
    write_cluster(fat_cluster + cluster_idx,
                  FAT.data() + cluster_idx*fat_entires_per_cluster);
  }

  // Gets a value from the in-memory FAT
  fat_entry get(size_t idx) {
    return FAT[idx];
  }
  
  // Loads the FAT from the filestore file
  void load() {
    for (size_t i=0; i < fat_clusters; ++i) {
      read_cluster(fat_cluster+i, FAT.data() + i*fat_entires_per_cluster);
    }
  }

  // Resets the FAT to all 0's
  void reset() {
    std::fill(FAT.begin(), FAT.end(), 0);
    persist();
  }

  // Write out the whole FAT
  void persist() {
    for(size_t cluster_idx=0; cluster_idx<fat_clusters; ++cluster_idx) {
      write_cluster(fat_cluster + cluster_idx,
                    FAT.data() + cluster_idx*fat_entires_per_cluster);
    }
  }
} FAT;

static const size_t directory_entry_size = 32;
struct directory_entry {
  uint32_t size{0};
  uint32_t starting_cluster{0};
  struct {
    // 0 if file, 1 if directory
    //unsigned char filetype : 1;
    unsigned char filetype;
  } flags;
  static const size_t max_name_len = directory_entry_size - sizeof(size)
    - sizeof(starting_cluster) - sizeof(flags);
  std::array<char, max_name_len> name{{'\0'}};
};

using directory = std::array<
  directory_entry, cluster_size / directory_entry_size>;

struct superblock {
  // other stuff I guess
  static const size_t superblock_pad_amt = cluster_size - sizeof(directory_entry);
  std::array<char, superblock_pad_amt> padding = {{0}};
  directory_entry root_directory_entry;
};

bool break_off_last_path_entry(const string& path,
                               string& parent_path,
                               string& child_name) {
  size_t last_delim_pos = path.find_last_of(directory_delim);
  if (last_delim_pos == string::npos) {
    return false;
  }

  parent_path = path.substr(0, last_delim_pos);
  child_name = path.substr(last_delim_pos+1);
  
  if (parent_path.empty()) {
    parent_path.push_back(directory_delim);
  }
  return true;
}

bool get_directory_entry_from_path(const string& path, directory_entry& ret) {
  // Base case: root directory
  if (path.length() == 1 && path[0] == directory_delim) {
    cout << "Reading superblock to find root directory" << endl;
    superblock s;
    read_cluster(superblock_cluster, &s);
    ret = s.root_directory_entry;
    return true;
  }

  string parent_path, child_name;
  if (!break_off_last_path_entry(path, parent_path, child_name)) {
    cerr << "get_directory_from_path called on invalid path" << endl;
    return false;
  }

  // Recurse; get parent's directory entry
  directory_entry parent_directory_entry;
  if (!get_directory_entry_from_path(parent_path, parent_directory_entry)) {
    cerr << "get_directory_entry_from_path called with nonexistant parent" << endl;
    return false;
  }
  if (!parent_directory_entry.flags.filetype) {
    cerr << "get_directory_entry_from_path called with nondirectory" << endl;
  }


  // If name ended with a /, we're done
  if (child_name.empty()) {
    cout << "Path ended with a /, we're done" << endl;
    ret = parent_directory_entry;
    return true;
  }

  // Get parent's directory from their directory entry
  directory parent_directory;
  cout << "Reading parent directory" << endl;
  read_cluster(parent_directory_entry.starting_cluster, &parent_directory);

  for (const directory_entry& d : parent_directory) {
    if (child_name == d.name.data()) {
      cout << "Found a match! " << child_name << " == " << d.name.data() << endl;
      ret = d;
      return true;
    }
  }
  cerr << "get_directory_from_path called with nonexistant child" << endl;
  return false;
}

static size_t find_next_free_cluster() {
  for (size_t i=0; i<fat_entries; ++i) {
    if (FAT.get(i) == 0) {
      return i+data_start_block;
    }
  }
  return -1;
}

static int adam_getattr(const char *cpath, struct stat *stbuf)
{
  string path(cpath);

  cerr << "ADAM: adam_getattr called on ``" << path << "''" << endl;

  memset(stbuf, 0, sizeof(struct stat));

  //stbuf->st_dev = 2; // TODO
  //stbuf->st_ino = 3; // TODO

  directory_entry dir_ent;
  if (!get_directory_entry_from_path(path, dir_ent)) {
    cerr << "getattr called on nonexistant file :c" << endl;
    return -ENOENT;
  }

  if (dir_ent.flags.filetype) {
    stbuf->st_mode = S_IFDIR | 0755;
    stbuf->st_nlink = 2;
  } else {
    stbuf->st_mode = S_IFREG | 0644;
    stbuf->st_nlink = 1;
  }

  stbuf->st_size = dir_ent.size;

  //stbuf->st_uid = getuid();
  //stbuf->st_gid = getgid();
  //stbuf->st_rdev = 0;

  //stbuf->st_blksize = 512;
  //stbuf->st_blocks = 3;
  //struct timespec time0 = {1455428262, 0};
  //stbuf->st_atim = time0;
  //stbuf->st_mtim = time0;
  //stbuf->st_ctim = time0;

  return 0;
}

static int adam_access(const char *cpath, int mask)
{
  string path(cpath);

  directory_entry dir_ent;
  if (get_directory_entry_from_path(path, dir_ent)) {
    if (dir_ent.flags.filetype || !(mask & X_OK)) {
      return 0;
    }
  }
  return -1;
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

  directory_entry dir_ent;
  if (!get_directory_entry_from_path(path, dir_ent)) {
    cout << "readdir called on nonexistant path" << endl;
    return -ENOENT;
  }
  if (!dir_ent.flags.filetype) {
    cout << "readdir called on file..." << endl;
    return -ENOENT;
  }

  directory dir;
  read_cluster(dir_ent.starting_cluster, &dir);

  for (size_t entry_num = offset; entry_num < dir.size(); ++entry_num) {
    const directory_entry& d = dir[entry_num];
    if (d.name[0] == '\0') {
      break;
    }
    if (filler(buf, d.name.data(), nullptr, entry_num+1) != 0) {
      return 0;
    }
  }
  return 0;
}

static int adam_mknod(const char *path, mode_t mode, dev_t rdev)
{
  fprintf(stderr, "ADAM: adam_mknod not implemented\n");
  return -ENOSYS;
}

static void mkdir_at_directory_entry(
    directory_entry& d, const string& name) {

  strncpy(d.name.data(), name.c_str(), d.name.size()-1);
  d.name[d.name.size()-1] = '\0';
  d.size = cluster_size;
  d.starting_cluster = find_next_free_cluster();
  d.flags.filetype = 1;

  FAT.set(d.starting_cluster-data_start_block, -1);

  directory dir;
  write_cluster(d.starting_cluster, &dir);

  cout << "Made new directory, put it at cluster " << d.starting_cluster << endl;
}

static int adam_mkdir(const char *cpath, mode_t mode)
{
  string path(cpath);

  cout << "mkdir called on path: " << path << endl;

  string parent_path, child_name;
  if (!break_off_last_path_entry(path, parent_path, child_name)) {
    cerr << "mkdir called on invalid path" << endl;
    return false;
  }

  if (child_name.length() >= directory_entry::max_name_len) {
    return -ENOENT;
  }

  directory_entry parent_dir_ent;
  if (!get_directory_entry_from_path(parent_path, parent_dir_ent)) {
    cout << "mkdir: parent path doesn't exist" << endl;
    return -1;
  }

  cout << "Mkdir directory entry lookup succeeded" << endl;

  directory parent_dir;
  read_cluster(parent_dir_ent.starting_cluster, &parent_dir);

  for (size_t dir_entry_num = 0;
       dir_entry_num < parent_dir.size();
       ++dir_entry_num) {
    directory_entry& d = parent_dir[dir_entry_num];
    if (d.name[0] == '\0') {
      mkdir_at_directory_entry(d, child_name);
      write_cluster(parent_dir_ent.starting_cluster, &parent_dir);
      cout << "Mkdir done" << endl;
      return 0;
    }
  }

  cout << "Parent directory too full to make new directory" << endl;

  return -1;
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
  static_assert(sizeof(directory) == cluster_size,
      "Size of directory is not cluster size");
  static_assert(cluster_size > directory_entry_size+512,
      "directory entry too big to store root directory entry in superblock");

  if (argc < 1) {
    cerr << "First argument should be name of file" << endl;
    return 2;
  }
  string filestorename(argv[1]);
  --argc;
  ++argv;
  
  cout << "Opening " << filestorename << " as FAT backend." << endl;
  filestore.open(filestorename,
      std::ios_base::binary|std::ios_base::in|std::ios_base::out);

  if (!filestore.good()) {
    cout << "File did not exist, initializing file with " << filestore_size <<
      " bytes" << endl;
    filestore.open(filestorename, std::ios_base::out | std::ios_base::binary);

    FAT.reset();

    superblock sup;
    mkdir_at_directory_entry(sup.root_directory_entry, "/");
    write_cluster(superblock_cluster, &sup);

    filestore.close();
    filestore.open(filestorename,
        std::ios_base::in | std::ios_base::out | std::ios_base::binary);
  } else {
    FAT.load();
  }

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
