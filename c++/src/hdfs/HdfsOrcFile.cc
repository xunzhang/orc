/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <string>
#include <memory>

#include "hdfs/hdfs.h"

#include "orc/OrcFile.hh"

#include "Adaptor.hh"
#include "Exceptions.hh"

namespace orc {

  class HdfsFileInputStream : public InputStream {
  private:
    std::string filename ;
    hdfsFS fs;
    hdfsFile file;
    int bufferSize;
    short replication;
    tOffset blockSize;
    uint64_t totalLength;

  private:
    void init(hdfsFS _fs, std::string _filename) {
      fs = _fs;
      filename = _filename;
      file = hdfsOpenFile(fs, filename.c_str(), O_RDONLY,
                          bufferSize, replication, blockSize);
      if (!file) {
        throw ParseError("Can't open hdfs file " + filename);
      }
      // TODO: set totalLength
    }

  public:
    HdfsFileInputStream(hdfsFS _fs, std::string _filename,
                        int _bufferSize = 0,
                        int _replication = 0,
                        tOffset _blockSize = 0) 
        : bufferSize(_bufferSize),
        replication(_replication),
        blockSize(_blockSize) {
      init(_fs, _filename);
    }

    HdfsFileInputStream(hdfsFS _fs, const char * _filename,
                        int _bufferSize = 0,
                        int _replication = 0,
                        tOffset _blockSize = 0) 
        : bufferSize(_bufferSize),
        replication(_replication),
        blockSize(_blockSize) {
      init(_fs, std::string(_filename));
    }

    ~FileInputStream() {
      hdfsCloseFile(fs, file);
    }

    uint64_t getLength() const override {
      return totalLength;
    }

    uint64_t getNaturalReadSize() const override {
      return 128 * 1024;
    }

    void read(void* buf,
              uint64_t length,
              uint64_t offset) override {
      if (!buf) {
        throw ParseError("Buffer is null");
      }
    }

    const std::string& getName() const override {
      return filename;
    }
  }; // class HdfsFileInputStream

  std::unique_ptr<InputStream> readHdfsFile(const std::string& path) {
    return std::unique_ptr<InputStream>(new HdfsFileInputStream(path));
  }
} // namespace orc

#ifndef HAS_STOLL

  #include <sstream>

  int64_t std::stoll(std::string str) {
    int64_t val = 0;
    stringstream ss ;
    ss << str ;
    ss >> val ;
    return val;
  }

#endif

