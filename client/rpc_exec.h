// Copyright (c) 2014 The IndexFS Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#ifndef _INDEXFS_CLIENT_RPCREDIRECT_H_
#define _INDEXFS_CLIENT_RPCREDIRECT_H_

#define EXEC_WITH_RETRY_TRY()                                 \
  int num_redirects = 0;                                      \
  while (num_redirects++ <= kNumRedirect) {                   \
    srv_id_ = dir_idx_->SelectServer(oid.obj_name);           \
    try                                                       \

#define EXEC_WITH_RETRY_CATCH()                               \
    catch (ServerRedirectionException &sx) {                  \
      dir_idx_->Update(sx.dmap_data);                         \
      continue;                                               \
    }                                                         \
    catch (IOError &ioe) {                                    \
      return Status::IOError(ioe.message);                    \
    }                                                         \
    catch (ServerInternalError &sie) {                        \
      return Status::Corruption(sie.message);                 \
    }                                                         \
    catch (UnrecognizedDirectoryError &ude) {                 \
      return Status::Corruption("Unrecognized directory id"); \
    }                                                         \
    catch (apache::thrift::TException &tx) {                  \
      LOG(FATAL) << "RPC exception: " << tx.what();           \
      abort();                                                \
    }                                                         \
  }                                                           \
  return Status::BufferFull("Too many redirection");          \

#endif /* _INDEXFS_CLIENT_RPCREDIRECT_H_ */
