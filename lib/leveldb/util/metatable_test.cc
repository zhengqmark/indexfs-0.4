// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table.h"
#include "leveldb/env.h"
#include "leveldb/options.h"
#include <stdio.h>
#include <vector>
#include <sys/stat.h>
#include <sys/types.h>

using namespace leveldb;

typedef struct MetaDB_key {
    uint64_t parent_id;
    long int partition_id;
    char name_hash[128];
} metadb_key_t;

typedef struct {
    struct stat statbuf;
    int state;
    size_t objname_len;
    char* objname;
    size_t realpath_len;
    char* realpath;
} metadb_val_header_t;

void FindKey(std::string filename) {
    Options options;
    RandomAccessFile* file;
    uint64_t file_size;
    Status s;
    s = Env::Default()->NewRandomAccessFile(filename, &file);
    if (!s.ok()) {
      printf("Cannot open file\n");
      return;
    }
    s = Env::Default()->GetFileSize(filename, &file_size);
    if (!s.ok()) {
      printf("Cannot get file size\n");
      return;
    }
    printf("%s\n", filename.c_str());
    Table* table;
    Table::Open(options, file, file_size, &table);
    Iterator* iter = table->NewIterator(ReadOptions());
    for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
      const metadb_key_t* tkey = (const metadb_key_t*) iter->key().data();
      const metadb_val_header_t* tval = (const metadb_val_header_t*) iter->value().data();

      if (tkey->parent_id == 0) {
         printf("%ld %ld %s\n",
                tkey->parent_id, tkey->partition_id, tkey->name_hash);
         const char* objname = iter->value().data()+sizeof(metadb_val_header_t);
         printf("%ld %s\n", tval->statbuf.st_ino, objname);
      }
    }
    delete iter;
    delete table;
    delete file;
}

int main(int argc, char** argv) {
    std::vector<std::string> result;
    Status s;
    std::string dirname(argv[1]);
    s = Env::Default()->GetChildren(dirname, &result);

    for (int i=0; i < result.size(); ++i) {
      if (result[i].find(".sst") != std::string::npos
       && result[i].find(".crc") == std::string::npos)
        FindKey(dirname+std::string("/")+result[i]);
    }
    return 0;
}
