// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/env.h"
#include "env/env_impl.h"

namespace leveldb {

Env::~Env() {
}

Env* Env::Default() {
  return GetOrNewPosixEnv();
}

SequentialFile::~SequentialFile() {
}

RandomAccessFile::~RandomAccessFile() {
}

WritableFile::~WritableFile() {
}

Logger::~Logger() {
}

FileLock::~FileLock() {
}

void Log(Logger* info_log, const char* format, ...) {
  if (info_log != NULL) {
    va_list ap;
    va_start(ap, format);
    info_log->Logv(format, ap);
    va_end(ap);
  }
}

static Status DoWriteStringToFile(Env* env, const Slice& data,
                                  const std::string& fname,
                                  bool should_sync) {
  WritableFile* file;
  Status s = env->NewWritableFile(fname, &file);
  if (!s.ok()) {
    return s;
  }
  s = file->Append(data);
  if (s.ok() && should_sync) {
    s = file->Sync();
  }
  if (s.ok()) {
    s = file->Close();
  }
  delete file;  // Will auto-close if we did not close above
  if (!s.ok()) {
    env->DeleteFile(fname);
  }
  return s;
}

Status WriteStringToFile(Env* env, const Slice& data,
                         const std::string& fname) {
  return DoWriteStringToFile(env, data, fname, false);
}

Status WriteStringToFileSync(Env* env, const Slice& data,
                             const std::string& fname) {
  return DoWriteStringToFile(env, data, fname, true);
}

Status ReadFileToString(Env* env, const std::string& fname, std::string* data) {
  data->clear();
  SequentialFile* file;
  Status s = env->NewSequentialFile(fname, &file);
  if (!s.ok()) {
    return s;
  }
  static const int kBufferSize = 8192;
  char* space = new char[kBufferSize];
  while (true) {
    Slice fragment;
    s = file->Read(kBufferSize, &fragment, space);
    if (!s.ok()) {
      break;
    }
    data->append(fragment.data(), fragment.size());
    if (fragment.empty()) {
      break;
    }
  }
  delete[] space;
  delete file;
  return s;
}

EnvWrapper::~EnvWrapper() {
}

bool EndsWith(const std::string& fname,
              const std::string& suffix) {
  size_t n = suffix.length();
  if (fname.length() < n) {
    return false;
  }
  return fname.compare(fname.length() - n, n, suffix) == 0;
}

bool StartsWith(const std::string& fname,
                const std::string& prefix) {
  size_t n = prefix.length();
  if (fname.length() < n) {
    return false;
  }
  return fname.compare(0, n, prefix) == 0;
}

bool LastComponentStartsWith(const std::string& fname,
                             const std::string& prefix) {
  size_t p = fname.rfind('/');
  if (p == std::string::npos) {
    p = -1;
  }
  ++p;
  size_t n = prefix.length();
  if (fname.length() - p < n) {
    return false;
  }
  return fname.compare(p, n, prefix) == 0;
}

bool OnHDFS(const std::string& fname) {
  return EndsWith(fname, E_LOG) ||
         EndsWith(fname, E_SST) ||
         EndsWith(fname, E_DAT) ||
         LastComponentStartsWith(fname, F_DSC);
}

bool OnRados(FileType type) {
  switch (type) {
  case kTableFile:
  case kLogFile:
  case kDataFile:
  case kDescriptorFile:
    return true;
  default:
    return false;
  }
}

FileType ResolveFileType(const std::string& fname) {
  if (LastComponentStartsWith(fname, F_DSC)) {
    return kDescriptorFile;
  }
  if (LastComponentStartsWith(fname, F_CUR)) {
    return kCurrentFile;
  }
  if (EndsWith(fname, E_SST)) {
    return kTableFile;
  }
  if (EndsWith(fname, E_LOG)) {
    return kLogFile;
  }
  if (EndsWith(fname, E_DAT)) {
    return kDataFile;
  }
  return kTempFile;
}

}  // namespace leveldb
