// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifdef RADOS
#include "leveldb/env/obj_set.h"
using leveldb::ObjEnv;
using leveldb::ObjEnvFactory;
#endif

#include "io_test/io_task.h"
#include <sstream>
#include <iomanip>
#include "common/config.h"
#include "common/options.h"
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <endian.h>
#include <mpi.h>
#include <gflags/gflags.h>
#include "metadb/dbtypes.h"
#include "util/leveldb_io.h"
#include "util/leveldb_reader.h"

namespace indexfs { namespace mpi {

DEFINE_string(sst_root, "/tmp/indexfs/leveldb",
    "Set root directory for parallel compaction");
DEFINE_string(sst_input, "/tmp/indexfs/leveldb/bk",
    "Set input directory for parallel compaction");
DEFINE_string(sst_output, "/tmp/indexfs/leveldb/l",
    "Set output directory for parallel compaction");

DEFINE_bool(sst_test, false, "Set to true to run unit test");
DEFINE_bool(sst_clean, true, "Set to false to keep the original SSTables");
DEFINE_bool(sst_local_mode, false, "Set to true to enable local major compaction");

namespace {

static const int kNumTablesPerRank = 2;
static const int kNumEntriesPerTable = 512;

class SSTCompTest: public IOTask {

  static inline
  void CheckStatus(const char* context, const Status& s) {
    if (!s.ok()) {
      throw IOError(context, s.ToString());
    }
  }

  int PrintSettings() {
    return printf("Test Settings:\n"
      "total mappers -> %d\n"
      "total reducers -> %d\n"
      "client radix -> %d\n"
      "backend_fs -> %s\n"
      "run_id -> %s\n"
      "sst_input -> %s\n"
      "sst_output -> %s\n"
      "keep input -> %s\n"
      "local mode -> %s\n",
      comm_sz_ / 2,
      comm_sz_ / 2,
      client_radix_,
      FLAGS_fs.c_str(),
      FLAGS_run_id.c_str(),
      FLAGS_sst_input.c_str(),
      FLAGS_sst_output.c_str(),
      GetBoolString(!FLAGS_sst_clean),
      GetBoolString(FLAGS_sst_local_mode));
  }

  static void GenerateSSTables(WritableFile* file,
                               MDBOptionsFactory* opt_factory,
                               int table_no,
                               int rank_no,
                               IOListener* L) {
    using mdb::MDBValue;
    using mdb::MDBKey;
    const Options& options = opt_factory->Get();
    const InternalKeyComparator& comp =
        reinterpret_cast<const InternalKeyComparator&>(*options.comparator);
    MDBMemTable mem_table(new MemTable(comp));
    for (int i = 0; i < kNumEntriesPerTable; ++i) {
      std::stringstream ss;
      ss << "r" << rank_no << "t" << table_no << "f" << i;
      std::string name = ss.str();
      mem_table->Add(0, kTypeValue,
          MDBKey(65536, 0, name).ToSlice(), MDBValue(name).ToSlice());
    }
    MDBTableBuilder table_builder(options, file);
    MDBIterator it(mem_table->NewIterator());
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      table_builder->Add(it->key(), it->value());
      if (L != NULL) {
        L->IOPerformed("add");
      }
    }
    CheckStatus("seal", table_builder.Seal());
  }

  struct Handler {
    virtual ~Handler() { }
    virtual void Init() { }
    virtual void Close() { }
    virtual void HandleEntry(const Slice& K, const Slice& V) = 0;
  };

# define HANDLER(name) \
  class name: virtual public Handler

  class MemTableSet {
   public:

    enum ReturnCode {
      kOkay = 0, kNoMoreEntries = 1,
    };

    MemTableSet(int num_tables) : num_entries_(0) {
      table_list_.resize(num_tables);
    }

    Iterator* NewIterator(const Options& options) {
      Iterator** children = new Iterator*[table_list_.size()];
      for (size_t t = 0; t < table_list_.size(); ++t) {
        children[t] = new MemTableInterator(&table_list_[t]);
      }
      Iterator* result = NewMergingIterator(options.comparator,
                                            children, table_list_.size());
      delete [] children;
      return result;
    }

    int TotalSize() { return num_entries_; }

    ReturnCode BulkInsert(int table, const char* buffer, size_t size) {
      Slice data(buffer, size);
      while (data.size() > 0) {
        int num_entries = DecodeFixed32(data.data());
        data.remove_prefix(4);
        if (num_entries == 0 && data.size() == 0) {
          return kNoMoreEntries; // No more entries
        }
        while (num_entries-- > 0) {
          const char* ptr = data.data();
          uint64_t key_size;
          GetVarint64(&data, &key_size);
          assert(key_size == 24);
          data.remove_prefix(key_size);
          uint64_t value_size;
          GetVarint64(&data, &value_size);
          data.remove_prefix(value_size);
          size_t size = VarintLength(key_size) + key_size
              + VarintLength(value_size) + value_size;
          assert(data.data() - ptr == size);
          char* buf = arena_.Allocate(size);
          memcpy(buf, ptr, size);
          num_entries_++;
          table_list_[table].push_back(Slice(buf, size));
        }
      }
      return kOkay; // More on the way
    }

   private:

    class MemTableInterator: public Iterator {
     private:
      size_t ptr;
      Slice key_;
      Slice value_;
      std::vector<Slice>* table_;

      void DecodeRaw() {
        if (Valid()) {
          Slice buffer = table_->at(ptr);
          uint64_t key_size;
          GetVarint64(&buffer, &key_size);
          key_ = Slice(buffer.data(), key_size);
          buffer.remove_prefix(key_size);
          uint64_t value_size;
          GetVarint64(&buffer, &value_size);
          value_ = Slice(buffer.data(), value_size);
          buffer.remove_prefix(value_size);
          assert(buffer.size() == 0);
        } else {
          key_ = Slice();
          value_ = Slice();
        }
      }

     public:
      MemTableInterator(std::vector<Slice>* table)
          : ptr(0), table_(table) {
      }

      virtual ~MemTableInterator() { }

      virtual void Next()               { ptr++; DecodeRaw(); }
      virtual void Prev()               { ptr--; DecodeRaw(); }
      virtual void Seek(const Slice& target) { abort(); } // Not supported
      virtual void SeekToFirst()        { ptr = 0; DecodeRaw(); }
      virtual void SeekToLast()         { ptr = table_->size() - 1; DecodeRaw(); }
      virtual Slice key() const         { return key_; }
      virtual Slice internalkey() const { return internalkey(); }
      virtual Slice value()             { return value_; }
      virtual Status status() const     { return Status::OK(); }
      virtual bool Valid() const { return ptr >= 0 && ptr < table_->size(); }
    };

    typedef std::vector<std::vector<Slice> > TableList;

    int num_entries_;
    Arena arena_;
    TableList table_list_;

    // No copying allowed
    MemTableSet(const MemTableSet&);
    MemTableSet& operator=(const MemTableSet&);
  };

  static void DoMapperSideMergeSort(const std::string& input_dir,
                                   MDBOptionsFactory* opt_factory,
                                   Handler* handler,
                                   IOListener* L) {
    MDBTableSet set;
    CheckStatus("open", set.Open(opt_factory->Get(), input_dir));
    handler->Init();
    for (set->SeekToFirst(); set->Valid(); set->Next()) {
      if (set->value().size() > 8) {
        handler->HandleEntry(set->key(), set->value());
        if (L != NULL) {
          L->IOPerformed("map");
        }
      }
    }
    handler->Close();
  }

  static void DoReducerSideMergeSort(const std::string& output_dir,
                                     int num_mappers,
                                     MDBOptionsFactory* opt_factory,
                                     Handler* handler,
                                     IOListener* L) {
    MemTableSet set(num_mappers);
    int done = 0;
    char buffer[1 << 18]; // 256K buffer
    while (done < num_mappers) {
      MPI_Status status;
      MPI_Probe(MPI_ANY_SOURCE, MPI_ANY_TAG, MPI_COMM_WORLD, &status);
      int count;
      MPI_Get_count(&status, MPI_CHAR, &count);
      assert(count < sizeof(buffer));
      MPI_Recv(buffer, count, MPI_CHAR,
          status.MPI_SOURCE, status.MPI_TAG, MPI_COMM_WORLD, MPI_STATUS_IGNORE);
      int table = status.MPI_SOURCE / 2;
      if (set.BulkInsert(table, buffer, count) == MemTableSet::kNoMoreEntries) {
        done++;
      }
    }
    MDBIterator it(set.NewIterator(opt_factory->Get()));
    handler->Init();
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      handler->HandleEntry(it->key(), it->value());
      if (L != NULL) {
        L->IOPerformed("reduce");
      }
    }
    handler->Close();
  }

  HANDLER(PrinterHandler) {
   public:
    void HandleEntry(const Slice& key, const Slice& value) {
      fprintf(stderr, "key %lu bytes, value %lu bytes\n",
          key.size(), value.size());
    }
  };

  HANDLER(ShuffleHandler) {
   public:

    ShuffleHandler(int radix,
                   const std::vector<Handler*>& children) :
                   radix_(radix) {
      children_.reserve(children.size());
      children_.insert(children_.begin(), children.begin(), children.end());
      counters_.resize(children.size(), 0);
    }

    virtual ~ShuffleHandler() {
      std::vector<Handler*>::iterator it;
      for (it = children_.begin(); it != children_.end(); ++it) {
        delete (*it);
      }
    }

    int TargetRank(const Slice& key) {
      // Assuming we are never going to have
      // more than 65536 number of clients
      const uint16_t* ptr16 =
          // Will shuffle according to the first 2 bytes of
          // the file name hash embedded in each key data structure
          reinterpret_cast<const uint16_t*>(key.data() + 8);

      // Use the most significant radix bits to do the shuffle
      return static_cast<int>(htobe16(*ptr16) >> (16 - radix_));
    }

    void HandleEntry(const Slice& key, const Slice& value) {
      int r = TargetRank(key);
      counters_[r]++;
      children_[r]->HandleEntry(key, value);
    }

    void Init() {
      std::vector<Handler*>::iterator it;
      for (it = children_.begin(); it != children_.end(); ++it) {
        (*it)->Init();
      }
    }

    void Close() {
      std::vector<Handler*>::iterator it;
      for (it = children_.begin(); it != children_.end(); ++it) {
        (*it)->Close();
      }
    }

   private:
    int radix_;
    std::vector<int> counters_;
    std::vector<Handler*> children_;

    // No copying allowed
    ShuffleHandler(const ShuffleHandler&);
    ShuffleHandler& operator=(const ShuffleHandler&);
  };

  HANDLER(MPISenderHandler) {
   public:

    MPISenderHandler(int target) : target_(target) { }

    void HandleEntry(const Slice& key, const Slice& value) {
      assert(key.size() == 0);
      char* data = const_cast<char*>(value.data());
      MPI_Send(data, value.size(), MPI_CHAR, target_, 0, MPI_COMM_WORLD);
    }

   private:
    int target_;

    // No copying allowed
    MPISenderHandler(const MPISenderHandler&);
    MPISenderHandler& operator=(const MPISenderHandler&);
  };

  HANDLER(BufferedSenderHandler) {
   public:

    BufferedSenderHandler(Handler* next_handler)
        : num_entries_(0),
          next_handler_(next_handler) {
      buffer_.reserve(kMaxBufferSize);
      PutFixed32(&buffer_, num_entries_);
    }

    virtual ~BufferedSenderHandler() {
      delete next_handler_;
    }

    void PrepareBuffer() {
      if (num_entries_ >= kMaxNumEntries) {
        char* buf = const_cast<char*>(buffer_.data());
        // Write the package size
        EncodeFixed32(buf, num_entries_);
        // A zero-length key implies a batch of entries
        // rather than a regular key-value pair
        next_handler_->HandleEntry(Slice(), buffer_);
        // Reset local buffer
        buffer_.resize(4);
        num_entries_ = 0;
      }
    }

    void HandleEntry(const Slice& key, const Slice& value) {
      PrepareBuffer();
      PutVarint64(&buffer_, key.size());
      buffer_.append(key.data(), key.size());
      PutVarint64(&buffer_, value.size());
      buffer_.append(value.data(), value.size());
      num_entries_++;
      assert(buffer_.size() <= kMaxBufferSize);
    }

    void Init() {
      next_handler_->Init();
    }

    void Close() {
      char* buf = const_cast<char*>(buffer_.data());
      EncodeFixed32(buf, num_entries_);
      if (num_entries_ != 0) {
        PutFixed32(&buffer_, 0); // Mark end of message
      }
      next_handler_->HandleEntry(Slice(), buffer_);
      next_handler_->Close();
    }

   private:
    int num_entries_;
    std::string buffer_;
    Handler* next_handler_;

    // Buffer 128K bytes
    enum {
      kMaxNumEntries = 1000, kTargetEntrySize = 128,
      // Max buffered data size with an additional entry for package heads
      kMaxBufferSize = (kMaxNumEntries * kTargetEntrySize) + kTargetEntrySize,
    };

    // No copying allowed
    BufferedSenderHandler(const BufferedSenderHandler&);
    BufferedSenderHandler& operator=(const BufferedSenderHandler&);
  };

  HANDLER(TableBuilderHandler) {
   public:

    TableBuilderHandler(const std::string& output_dir,
                        MDBOptionsFactory* opt_factory)
                      : num_(1),
                        manifest_num_(num_),
                        output_dir_(output_dir),
                        last_seq_(0),
                        has_first_key_(false),
                        builder_(NULL),
                        opt_factory_(opt_factory) {
      env_ = opt_factory_->Get().env;
    }

    virtual ~TableBuilderHandler() {
      delete builder_;
    }

    void PrepareTable() {
      if (builder_ != NULL) {
        if ((*builder_)->FileSize() >= DEFAULT_LEVELDB_SSTABLE_SIZE) {
          WriteTable();
        }
      }
      if (builder_ == NULL) {
        WritableFile* file;
        table_name_ = TableFileName(output_dir_, ++num_);
        CheckStatus("create", env_->NewWritableFile(table_name_, &file));
        builder_ = new MDBTableBuilder(opt_factory_->Get(), file);
      }
    }

    void HandleEntry(const Slice& key, const Slice& value) {
      PrepareTable();
      HandleKey(key);
      (*builder_)->Add(key, value);
    }

    void Close() {
      if (builder_ != NULL) {
        WriteTable();
        WriteManifest();
      }
    }

   private:

    void HandleKey(const Slice& key) {
      largest_key_.DecodeFrom(key);
      if (!has_first_key_) {
        has_first_key_ = true;
        first_key_.DecodeFrom(key);
      }
      last_seq_ = std::max(last_seq_, MDBHelper::FetchSeqNum(key));
    }

    void WriteTable() {
      assert(builder_ != NULL);
      // empty tables will be abandoned automatically
      // during table sealing
      CheckStatus("seal", builder_->Seal());
      if ((*builder_)->NumEntries() > 0) {
        uint64_t file_size;
        CheckStatus("fstat", env_->GetFileSize(table_name_, &file_size));
        assert(file_size > 0);
        edit_.AddFile(0, num_, file_size, first_key_, largest_key_);
      }
      delete builder_;
      builder_ = NULL;
      first_key_.Clear();
      largest_key_.Clear();
      has_first_key_ = false;
    }

    void WriteManifest() {
      edit_.SetLastSequence(last_seq_);
      std::string record;
      edit_.EncodeTo(&record);
      WritableFile* file;
      std::string fname = DescriptorFileName(output_dir_, manifest_num_);
      CheckStatus("create", env_->NewWritableFile(fname, &file));
      {
        LogWriter writer(file);
        CheckStatus("write", writer.AddRecord(record));
        CheckStatus("fsync", file->Sync());
      }
      delete file;
    }

    int num_;
    int manifest_num_;
    Env* env_;
    std::string output_dir_;
    VersionEdit edit_;
    SequenceNumber last_seq_;
    bool has_first_key_;
    InternalKey first_key_;
    InternalKey largest_key_;
    MDBTableBuilder* builder_;
    std::string table_name_;
    MDBOptionsFactory* opt_factory_;

    // No copying allowed
    TableBuilderHandler(const TableBuilderHandler&);
    TableBuilderHandler& operator=(const TableBuilderHandler&);
  };

  // Process type.
  enum Type {
    kMapper = 0, kReducer = 1,
  };

  Env* env_;
  int my_id_;
  int client_radix_;
  std::string dir_in_;
  std::string dir_out_;
  MDBOptionsFactory* opt_factory_;

  Env* InitSysEnv() {
    Env* env = Env::Default();
    if (!env->FileExists(FLAGS_sst_root)) {
      env->CreateDir(FLAGS_sst_root);
    }
    if (my_rank_ % 2 == kMapper) {
      if (FLAGS_sst_test) {
#       ifdef RADOS
          env = InitReadWriteRadosEnv(FLAGS_sst_root, dir_in_);
#       endif
        if (!env->FileExists(dir_in_)) {
          env->CreateDir(dir_in_);
        }
      } else {
#       ifdef RADOS
          if (!FLAGS_sst_local_mode) {
            env = InitReadOnlyRadosEnv(FLAGS_sst_root, dir_in_);
          } else {
            env = InitReadWriteRadosEnv(FLAGS_sst_root, dir_out_, dir_in_);
          }
#       endif
        assert(env->FileExists(dir_in_));
        if (FLAGS_sst_local_mode) {
          if (!env->FileExists(dir_out_)) {
            env->CreateDir(dir_out_);
          }
        }
      }
    } else if (!FLAGS_sst_local_mode) {
#     ifdef RADOS
        env = InitReadWriteRadosEnv(FLAGS_sst_root, dir_out_);
#     endif
      if (!env->FileExists(dir_out_)) {
        env->CreateDir(dir_out_);
      }
    }
    return env;
  }

# ifdef RADOS
  Env* InitReadWriteRadosEnv(const std::string& root,
                             const std::string& rw_home,
                             const std::string& ro_home=std::string()) {
    using leveldb::RadosOptions;
    RadosOptions opt;
    opt.read_only = false;
    opt.db_root = root.c_str();
    opt.db_home = rw_home.c_str();
    Env* env = GetOrNewRadosEnv(opt);
    if (!ro_home.empty()) {
      ObjEnv* obj_env = ObjEnvFactory::FetchObjEnv();
      assert(obj_env != NULL);
      Status s_ = obj_env->LoadSet(ro_home);
      if (!s_.ok()) {
        abort();
      }
      if (!Env::Default()->FileExists(ro_home)) {
        Env::Default()->CreateDir(ro_home);
      }
    }
    return env;
  }
# endif

# ifdef RADOS
  Env* InitReadOnlyRadosEnv(const std::string& root, const std::string& ro_home) {
    using leveldb::RadosOptions;
    std::string tmp_home = root + "/tmp";
    RadosOptions opt;
    opt.read_only = true;
    opt.db_root = root.c_str();
    opt.db_home = tmp_home.c_str();
    Env* env = GetOrNewRadosEnv(opt);
    ObjEnv* obj_env = ObjEnvFactory::FetchObjEnv();
    assert(obj_env != NULL);
    Status s_ = obj_env->LoadSet(ro_home);
    if (!s_.ok()) {
      abort();
    }
    if (!Env::Default()->FileExists(ro_home)) {
      Env::Default()->CreateDir(ro_home);
    }
    return env;
  }
# endif

 public:

  SSTCompTest(int my_rank, int comm_sz)
    : IOTask(my_rank, comm_sz),
      env_(NULL), client_radix_(-1), opt_factory_(NULL) {
    // Half of clients are Mappers, another half are Reducers
    my_id_ = my_rank_ / 2;
    std::stringstream ss1;
    ss1 << FLAGS_sst_input << my_id_;
    dir_in_ = ss1.str();
    std::stringstream ss2;
    ss2 << FLAGS_sst_output << my_id_;
    dir_out_ = ss2.str();
    while ((comm_sz >>= 1) != 0) { client_radix_++; }
    assert(client_radix_ >= 0);
    assert((1 << client_radix_) * 2 == comm_sz_);
  }

  virtual void Prepare() {
    env_ = InitSysEnv();
    opt_factory_ = new MDBOptionsFactory(env_);
    if (FLAGS_sst_test && my_rank_ % 2 == kMapper) {
      for (int i = 1; i <= kNumTablesPerRank; ++i) {
        WritableFile* file;
        std::string fname = TableFileName(dir_in_, i);
        CheckStatus("create", env_->NewWritableFile(fname, &file));
        GenerateSSTables(file, opt_factory_, i, my_id_, listener_);
      }
    }
  }

  virtual void Run() {
    if (my_rank_ % 2 == kMapper) {
      if (!FLAGS_sst_local_mode) {
        std::vector<Handler*> children;
        for (int i = 0; i < comm_sz_; i += 2) {
          children.push_back(
              new BufferedSenderHandler(new MPISenderHandler(i + 1)));
        }
        ShuffleHandler h(client_radix_, children);
        DoMapperSideMergeSort(dir_in_, opt_factory_, &h, listener_);
      } else {
        TableBuilderHandler h(dir_out_, opt_factory_);
        DoMapperSideMergeSort(dir_in_, opt_factory_, &h, listener_);
      }
    } else if (!FLAGS_sst_local_mode) {
      TableBuilderHandler h(dir_out_, opt_factory_);
      DoReducerSideMergeSort(dir_out_, comm_sz_ / 2, opt_factory_, &h, listener_);
    }
  }

  virtual void Clean() {
    if (my_rank_ % 2 == kMapper) {
      if (FLAGS_sst_clean) {
        std::vector<std::string> names;
        CheckStatus("list", env_->GetChildren(dir_in_, &names));
        std::vector<std::string>::iterator it;
        for (it = names.begin(); it != names.end(); ++it) {
          if (IsSSTableFile(*it)) {
            // For RADOS env, this call will always return error
            // since the corresponding "set" is read only.
            // However, the underlying object will be deleted anyway.
            std::string fname = dir_in_ + "/" + *it;
            env_->DeleteFile(fname);
          }
        }
      }
      if (FLAGS_sst_local_mode) {
#       ifdef RADOS
        ObjEnv* obj_env = ObjEnvFactory::FetchObjEnv();
        assert(obj_env != NULL);
        CheckStatus("sync", obj_env->SyncSet(dir_out_));
#       endif
      }
    } else if (!FLAGS_sst_local_mode) {
#     ifdef RADOS
      ObjEnv* obj_env = ObjEnvFactory::FetchObjEnv();
      assert(obj_env != NULL);
      CheckStatus("sync", obj_env->SyncSet(dir_out_));
#     endif
    }
  }

  virtual bool CheckPrecondition() {
    if (IO_ == NULL || LOG_ == NULL) {
      return false; // err has already been printed elsewhere
    }
    if (comm_sz_ % 2 != 0) {
      return false;
    }
    my_rank_ == 0 ? PrintSettings() : 0;
    // All will check, yet only the zeroth process will do the printing
    return true;
  }

};

} /* anonymous namespace */

IOTask* IOTaskFactory::GetCompactionTestTask(int my_rank, int comm_sz) {
  return new SSTCompTest(my_rank, comm_sz);
}

} /* namespace mpi */ } /* namespace indexfs */
