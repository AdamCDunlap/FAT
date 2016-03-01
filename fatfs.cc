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
#include <cassert>
#include <iterator>
#include <memory>

using std::string;
using std::cerr;
using std::endl;
using std::cout;

static const size_t filestore_size = 10*1024*1024;
static const size_t cluster_size = 4*1024;

string adam_file_contents("Hello there\n");
static std::fstream filestore;

using cluster_idx_t = uint32_t;
using filesize_t = uint32_t;
static const size_t cluster_idx_size = sizeof(cluster_idx_t);
static const size_t fat_entries = 1 +   (filestore_size - cluster_size - 1)
                                   / (cluster_size + cluster_idx_size);
static const size_t fat_size = fat_entries * cluster_idx_size;
static const size_t fat_clusters = 1 + (fat_size - 1) / cluster_size;
static const size_t padded_fat_size = fat_clusters * cluster_size;
static const size_t padded_fat_entries = padded_fat_size / cluster_idx_size;
static const size_t fat_entires_per_cluster = cluster_size / cluster_idx_size;

static const size_t superblock_cluster = 0;
static const size_t fat_cluster = 1;
// Block at which the root directory starts
static const size_t data_start_block = fat_cluster + fat_clusters;

static const char directory_delim = '/';
static const size_t directory_size = cluster_size;

static const cluster_idx_t end_of_file_cluster_marker
    = std::numeric_limits<cluster_idx_t>::max();

static const string term_red = "[0;31m";
static const string term_yellow = "[0;33m";
static const string term_reset = "[0;0m";

// Writes 1 cluster to the filestore from the pointer
template<typename T>
void write_cluster(size_t cluster_num, const T* data) {
  //cout << "Writing to cluster " << cluster_num << endl;
  //if (cluster_num == superblock_cluster) {
  //  cout << "[32;1mWARNING: Writing to superblock cluster![0;0m" << endl;
  //}

  filestore.seekp(cluster_num * cluster_size);
  filestore.write(reinterpret_cast<const char*>(data), cluster_size);
  filestore.flush();
}
void clear_cluster(size_t cluster_num) {
  static std::array<char, cluster_size> zeros = {{0}};
  write_cluster(cluster_num, zeros.data());
}

// Reads 1 cluster from the filestore into the pointer
template<typename T>
void read_cluster(size_t cluster_num, T* data) {
  //cout << "Reading from cluster " << cluster_num << endl;
  filestore.seekg(cluster_num*cluster_size);
  filestore.read(reinterpret_cast<char*>(data), cluster_size);
}

class disk_full_error_t {} disk_full_error;

class FAT_t {
  std::vector<cluster_idx_t> FAT;
 public:

  FAT_t() : FAT(padded_fat_entries)
  {
  }

  // Sets a value in the in-memory FAT. DOES NOT persist
  void weak_set(size_t idx, cluster_idx_t val) {
    assert(idx >= data_start_block);
    //cout << "Writing " << val << " to FAT at index " << idx << endl;
    if (idx == val) {
      cerr << "ERROR: Making loop in FAT" << endl;
      exit(3);
    }
    FAT[idx-data_start_block] = val;
  }

  // Sets a value in the FAT and persists the change to disk
  void set(size_t idx, cluster_idx_t val) {
    weak_set(idx, val);

    const size_t cluster_idx = idx / cluster_size;
    write_cluster(fat_cluster + cluster_idx,
                  FAT.data() + cluster_idx*fat_entires_per_cluster);
  }

  void set_end(size_t idx) {
    set(idx, end_of_file_cluster_marker);
  }

  // Gets a value from the in-memory FAT
  cluster_idx_t get(size_t idx) {
    return FAT[idx-data_start_block];
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

static cluster_idx_t get_free_cluster() {
  // TODO: Use linked list. Change fill to iota in reset()
  for (size_t i=data_start_block; i<fat_entries+data_start_block; ++i) {
    if (FAT.get(i) == 0) {
      return i;
    }
  }
  //cerr << "Can't find any free clusters!" << endl;
  throw disk_full_error;
  return end_of_file_cluster_marker;
}

static const size_t directory_entry_size = 32;
struct directory_entry {
  filesize_t size{0};
  cluster_idx_t starting_cluster{0};
  struct flags {
    //unsigned char filetype : 1;
    enum { FILE, DIRECTORY } filetype;
  } flags;
  static const size_t max_name_len = directory_entry_size - sizeof(size)
    - sizeof(starting_cluster) - sizeof(flags);
  std::array<char, max_name_len> name{{'\0'}};

  bool is_valid() {
    return name[0] != '\0';
  }
  void make_invalid() {
    name[0] = '\0';
  }
};


struct superblock {
  // other stuff I guess?
  static const size_t superblock_pad_amt
    = cluster_size - sizeof(directory_entry);
  std::array<char, superblock_pad_amt> padding = {{0}};
  directory_entry root_directory_entry;
};

static void populate_dirent_with_dir(const string& name, directory_entry& d) {
  d.starting_cluster = get_free_cluster();
  d.flags.filetype = directory_entry::flags::DIRECTORY;
  d.size = directory_size;

  strncpy(d.name.data(), name.c_str(), d.name.size()-1);
  d.name[d.name.size()-1] = '\0';

  clear_cluster(d.starting_cluster);
  //cout << "Setting FAT entry to end: " << d.starting_cluster << endl;
  FAT.set_end(d.starting_cluster);
}

// A directory can iterate through its directory entries
class directory {
  cluster_idx_t starting_cluster;
 public:
  static const size_t files_in_dir_cluster
    = cluster_size / directory_entry_size;

  class iterator
      : public std::iterator<std::forward_iterator_tag, directory_entry>
  {

    friend class directory;
    using dirent_array = std::array<directory_entry, files_in_dir_cluster>;

    // Data members

    // Pointer to currently-examined cluster
    std::shared_ptr<dirent_array> data;
    // Index within the cluster
    size_t dir_idx = 0;
    // Cluster's number
    cluster_idx_t cur_cluster;

    iterator(cluster_idx_t starting_cluster)
        : cur_cluster(starting_cluster)
    {
      //reread();
    }

    // Move to next entry, valid or not
    void move_to_next_entry() {
      if (!data) {
        reread();
      }

      ++dir_idx;

      if (dir_idx == files_in_dir_cluster) {
        //cout << "Moving to next cluster" << endl;
        cur_cluster = FAT.get(cur_cluster);
        dir_idx = 0;
        if (cur_cluster == end_of_file_cluster_marker) {
          // The iterator's at the end, no point in keeping the old data around
          data.reset();
        } else {
          reread();
        }
      }
    }

    // If current entry is valid, do nothing. Otherwise, move until the current
    // entry is valid or we're at the end
    void move_to_valid_entry_or_end() {
      while (cur_cluster != end_of_file_cluster_marker
               && !(*this)->is_valid()) {
        move_to_next_entry();
      }
    }

    // Reread from disk
    void reread() {
      if (!data.unique()) {
        //cout << "Making new shared dirent array. use count = " << data.use_count() << " unique = " << data.unique() << endl;
        data = std::make_shared<dirent_array>();
      }
      read_cluster(cur_cluster, data->data());
    }
    // Write to disk
    void persist() {
      write_cluster(cur_cluster, data->data());
    }


   public:
    iterator() = default;
    iterator(const iterator&) = default;
    iterator& operator=(const iterator&) = default;
    ~iterator() = default;

    directory_entry& operator*() {
      if (!data) reread();
      return (*data)[dir_idx];
    }

    directory_entry* operator->() {
      return &operator*();
    }

    // Skips over invalid directory entries
    iterator& operator++() {
      move_to_next_entry();
      move_to_valid_entry_or_end();
      return *this;
    }

    bool operator==(const iterator& other) {
      return dir_idx == other.dir_idx && cur_cluster == other.cur_cluster;
    }
    bool operator!=(const iterator& other) {
      return !(*this == other);
    }
  };

  directory(const directory_entry& d)
    : starting_cluster(d.starting_cluster)
  {
  }

  iterator begin() {
    iterator ret(starting_cluster);
    ret.move_to_valid_entry_or_end();
    return ret;
  }

  // Returns an iterator that may start at an invalid dirent
  iterator raw_begin() {
    return iterator(starting_cluster);
  }

  iterator end() {
    return iterator(end_of_file_cluster_marker);
  }

  // Finds an empty space in the directory and puts the directory_entry there
  void add_directory_entry(const directory_entry& d) {
    iterator prev_it(424242); // Invalid so we can check for the magic number

    // This loop will always run at least once to prev_it will be set
    for (iterator it = raw_begin(); it != end(); it.move_to_next_entry()) {
      if (!it->is_valid()) {
        it.reread();
        *it = d;
        it.persist();
        return;
      }
      prev_it = it;
    }

    //cout << "add_directory_entry: Creating new cluster" << endl;
    
    // We need to create a new cluster! prev_it points to the last directory
    // entry in the last cluster
    cluster_idx_t new_cluster = get_free_cluster();
    //cout << "add_directory_entry: Got a free cluster: " << new_cluster << endl;
    FAT.set(prev_it.cur_cluster, new_cluster);
    FAT.set_end(new_cluster);
    clear_cluster(new_cluster);

    //cout << "add_directory_entry: moving to next entry" << endl;
    // Now that there's another cluster, prev_it will move to its first entry
    prev_it.move_to_next_entry();
    *prev_it = d;
    //cout << "add_directory_entry: Persisting the iterator" << endl;
    prev_it.persist();
    //cout << "add_directory_entry: Done" << endl;
  }

  // XXX: Doesn't delete clusters if the directory can fit in fewer
  void delete_directory_entry(iterator location) {
    location.reread();
    location->make_invalid();
    location.persist();
  }

  void replace_directory_entry(iterator location, const directory_entry& d) {
    location.reread();
    *location = d;
    location.persist();
  }

  //static int mkdir_at_directory_entry(
  //    directory_entry& d, const string& name) {

  //  d.starting_cluster = get_free_cluster();

  //  strncpy(d.name.data(), name.c_str(), d.name.size()-1);
  //  d.name[d.name.size()-1] = '\0';
  //  d.size = directory_size;
  //  d.flags.filetype = 1;

  //  directory dir;
  //  dir.write(dir.begin());

  //  FAT.set_end(d.starting_cluster);

  //  cout << "Made new directory, put it at cluster " << d.starting_cluster << endl;
  //  return 0;
  //}

  //void add_cluster(directory_entry& dir_ent, cluster_idx_t last_cluster) {
  //  cluster_idx_t new_cluster = get_free_cluster();

  //  cout << "Putting new cluster at " << new_cluster << endl;
  //  FAT.set(last_cluster, new_cluster);
  //  FAT.set_end(new_cluster);

  //  dir_ent.size += directory_size;

  //  clear_cluster(new_cluster);
  //}

  void make_child_dir(const string& name) {
    directory_entry new_dir_ent;
    populate_dirent_with_dir(name, new_dir_ent);
    add_directory_entry(new_dir_ent);
  }
};

// Makes a directory entry for a directory with the given name and allocates
// disk space for it
directory_entry construct_dir_dir_ent(const string& name) {
  directory_entry new_dir_ent;
  new_dir_ent.starting_cluster = get_free_cluster();
  new_dir_ent.flags.filetype = directory_entry::flags::DIRECTORY;
  new_dir_ent.size = directory_size;

  strncpy(new_dir_ent.name.data(), name.c_str(), new_dir_ent.name.size()-1);
  new_dir_ent.name[new_dir_ent.name.size()-1] = '\0';

  clear_cluster(new_dir_ent.starting_cluster);
  return new_dir_ent;
}

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
    //cout << "Reading superblock to find root directory" << endl;
    superblock s;
    read_cluster(superblock_cluster, &s);
    ret = s.root_directory_entry;
    //cout << "Root directory is at cluster " << ret.starting_cluster << endl;
    return true;
  }

  string parent_path, child_name;
  if (!break_off_last_path_entry(path, parent_path, child_name)) {
    //cerr << "get_directory_from_path called on invalid path" << endl;
    return false;
  }

  // Recurse; get parent's directory entry
  directory_entry parent_directory_entry;
  if (!get_directory_entry_from_path(parent_path, parent_directory_entry)) {
    //cerr << "get_directory_entry_from_path called with nonexistant parent (full path " << path << endl;
    return false;
  }
  //cout << "Got parent directory entry" << endl;
  if (parent_directory_entry.flags.filetype
      != directory_entry::flags::DIRECTORY) {
    //cerr << "get_directory_entry_from_path called with nondirectory " << path << "<- its flags are " << parent_directory_entry.flags.filetype << endl;
    return false;
  }


  // If name ended with a /, we're done
  if (child_name.empty()) {
    //cout << "Path ended with a /, we're done" << endl;
    ret = parent_directory_entry;
    return true;
  }

  //cout << "Creating parent directory object" << endl;
  directory parent_directory(parent_directory_entry);
  for (const directory_entry& parent_dir_ent : parent_directory) {
    //cout << "Iterating through dirents of parent" << endl;
    if (child_name == parent_dir_ent.name.data()) {
      ret = parent_dir_ent;
      return true;
    }
  }
  //cerr << "get_directory_from_path called with nonexistant child" << endl;
  return false;
}

static int adam_getattr(const char *cpath, struct stat *stbuf)
{
  string path(cpath);

  cout<<term_yellow << "ADAM: adam_getattr called on ``" << path << "''" << term_reset<<endl;

  memset(stbuf, 0, sizeof(struct stat));

  directory_entry dir_ent;
  if (!get_directory_entry_from_path(path, dir_ent)) {
    cerr << "[31;0mERROR: getattr called on nonexistant file " << path << "[0;m" << endl;
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
  cout << term_yellow << "access called on " << path << term_reset << endl;

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
                       off_t signed_offset, struct fuse_file_info *fi)
{
  string path(cpath);
  size_t offset = signed_offset; // why is it given to us signed? :/
  cout <<term_yellow<< "ADAM: adam_readdir called on ``" << path << "'' with offset " << offset <<term_reset<< endl;

  directory_entry dir_ent;
  if (!get_directory_entry_from_path(path, dir_ent)) {
    cout <<term_red<< "readdir called on nonexistant path" << term_reset<<endl;
    return -ENOENT;
  }
  if (!dir_ent.flags.filetype) {
    cout <<term_red<< "readdir called on file..." <<term_reset<< endl;
    return -ENOTDIR;
  }

  directory dir(dir_ent);

  // Loop over all the directory entries, PLUS "." and ".."
  // The first two times, we'll use "." and "..", then start incrementing
  // dir_iter to walk through the actual directory entries. If we see an
  // invalid directory entry, we just ignore it. Also, if cur_fileno is less
  // than offset, then that means we've already returned that file so we should
  // skip it as well.
  directory::iterator dir_iter = dir.begin();
  for (size_t cur_fileno = 0; cur_fileno <= 1 || dir_iter != dir.end();) {
    string name;
    switch(cur_fileno) {
      case 0: name = "."; break;
      case 1: name = ".."; break;
      default:
              name = dir_iter->name.data();
              ++dir_iter;
              if (name[0] == '\0') {
                cerr << "ERROR: dir iter returned invalid entry on fileno " << cur_fileno << endl;
                exit(4);
                continue;
              }
              break;
    }
    // Filler will fill the buffer for us using magic. If it returns nonzero, it
    // means it wants us to return 0 as well. Then we'll be called back with
    // cur_fileno+1, so we start returning the next entry properly
    if (cur_fileno >= offset) {
      if (filler(buf, name.c_str(), nullptr, cur_fileno+1) != 0) {
        return 0;
      }
    }
    ++cur_fileno;
  }
  return 0;

  // meh
  //size_t cur_fileno = 0;
  //if (cur_fileno >= offset) {
  //  if (filler(buf, ".", nullptr, cur_fileno+1) != 0) {
  //    return 0;
  //  }
  //}
  //++cur_fileno;
  //if (cur_fileno >= offset) {
  //  if (filler(buf, "..", nullptr, cur_fileno+1) != 0) {
  //    return 0;
  //  }
  //}
  //++cur_fileno;

  //directory dir(dir_ent);
  //for (const directory_entry& d : dir) {
  //  if (cur_fileno >= offset) {
  //    if (d.name[0] == '\0') {
  //      break;
  //    }

  //    if (filler(buf, d.name.data(), nullptr, cur_fileno+1) != 0) {
  //      return 0;
  //    }
  //  }
  //  ++cur_fileno;
  //}
  //return 0;
}

static int adam_mknod(const char *path, mode_t mode, dev_t rdev)
{
  fprintf(stderr, "ADAM: adam_mknod not implemented\n");
  return -ENOSYS;
}


static int adam_mkdir(const char *cpath, mode_t mode)
{
  string path(cpath);

  //cout << "mkdir called on path: " << path << endl;

  string parent_path, child_name;

  if (child_name.length() >= directory_entry::max_name_len) {
    //cout << "mkdir: filename too long" << endl;
    return -ENAMETOOLONG;
  }

  if (!break_off_last_path_entry(path, parent_path, child_name)) {
    cerr <<term_red<< "mkdir called on invalid path" <<term_reset<< endl;
    return -ENOENT;
  }

  directory_entry parent_dir_ent;
  if (!get_directory_entry_from_path(parent_path, parent_dir_ent)) {
    cout <<term_red<<"mkdir: parent path doesn't exist"<<term_reset << endl;
    return -ENOENT;
  }

  //cout << "Mkdir directory entry lookup succeeded" << endl;
  directory parent_dir(parent_dir_ent);
  try {
    parent_dir.make_child_dir(child_name);
  } catch (disk_full_error_t) {
    return -ENOSPC;
  }
  return 0;


//  cluster_idx_t parent_dir_cluster = parent_dir_ent.starting_cluster;
//  cluster_idx_t last_parent_dir_cluster;
//  // Get parent's directory from their directory entry
//  do {
//      directory parent_directory(parent_dir_ent);
//      for (directory_entry& d : parent_directory) {
//        if (!d.is_valid()) {
//          int err = directory::mkdir_at_directory_entry(d, child_name);
//          if (err) {
//            cout << "mkdir: mkdir_at_directory_entry failed, returning " << err << endl;
//            return err;
//          }
//          write_cluster(parent_dir_cluster, &parent_directory);
//          cout << "Mkdir done" << endl;
//          return 0;
//        }
//      }
//
//    // TODO: this should be abstracted somewhat
//
//    cout << "Parent directory too full to make new directory, "
//            "so we'll make it bigger! " << endl;
//
//    cluster_idx_t newparentcluster = get_free_cluster();
//
//    cout << "Putting new cluster at " << newparentcluster << endl;
//    FAT.set(last_parent_dir_cluster, newparentcluster);
//    FAT.set(newparentcluster, end_of_file_cluster_marker);
//    parent_dir_cluster = newparentcluster;
//
//    parent_dir_ent.size += directory_size;
//
//    directory parent_dir_extended;
//    write_cluster(parent_dir_cluster, &parent_dir_extended);
//    cout << "Done making new cluster" << endl;
//  } while(true);
//
//
//
//
//
//  //directory parent_dir;
//  //read_cluster(parent_dir_ent.starting_cluster, &parent_dir);
//
//  //for (size_t dir_entry_num = 0;
//  //     dir_entry_num < parent_dir.size();
//  //     ++dir_entry_num) {
//  //  directory_entry& d = parent_dir[dir_entry_num];
//  //  if (d.name[0] == '\0') {
//  //    int err = mkdir_at_directory_entry(d, child_name, parent_dir_ent);
//  //    if (err) {
//  //      return err;
//  //    }
//  //    write_cluster(parent_dir_ent.starting_cluster, &parent_dir);
//  //    cout << "Mkdir done" << endl;
//  //    return 0;
//  //  }
//  //}
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
  static_assert(padded_fat_entries * cluster_idx_size == padded_fat_size,
      "cluster size not multiple of fat entry size");
  static_assert(cluster_size > directory_entry_size+512,
      "directory entry too big to store root directory entry in superblock");

  if (argc <= 1) {
    cerr << "First argument should be name of backing file" << endl;
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
    populate_dirent_with_dir("/", sup.root_directory_entry);
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
  return fuse_main(argc, argv, &adam_oper, nullptr);
}
