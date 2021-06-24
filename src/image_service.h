/*
   Copyright The Overlaybd Authors

   Licensed under the Apache License, Version 2.0 (the "License");
   you may not use this file except in compliance with the License.
   You may obtain a copy of the License at

       http://www.apache.org/licenses/LICENSE-2.0

   Unless required by applicable law or agreed to in writing, software
   distributed under the License is distributed on an "AS IS" BASIS,
   WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
   See the License for the specific language governing permissions and
   limitations under the License.
*/

#pragma once

#include <string>
#include "config.h"
#include "overlaybd/fs/forwardfs.h"
#include "overlaybd/string-keyed.h"

namespace FileSystem {
    class RefObj;
    static unordered_map_string_key<RefObj *> opened;

    class RefObj: public ForwardFile_Ownership {
    public:
        RefObj(IFile *file, const std::string &s): ForwardFile_Ownership(file, true) {
            LOG_DEBUG("new ref obj: `", s);
            ref_count = 1;
            key = s;
            opened[s] = this;
        }
        virtual int close() override {
            ref_count--;
            if (ref_count == 0) {
                LOG_DEBUG("delete ref obj: `", key);
                opened.erase(key);
                delete this;
            }
            return 0;
        }
        std::string key;
        int ref_count = 0;
        IFile* get_file() {
            return m_file;
        }
    };

    class RefFile: public ForwardFile {
    public:
        RefFile(IFile *file): ForwardFile(file) {
        }

        ~RefFile() {
            close();
        }
        IFile* get_file() {
            return ((RefObj*)(m_file))->get_file();
        }
    };

    RefFile* get_ref_file(const std::string &key);
    RefFile* new_ref_file(IFile *file, const std::string &key);
}

typedef enum {
    io_engine_psync,
    io_engine_libaio,
    io_engine_posixaio
} IOEngineType;

struct GlobalFs {
    FileSystem::IFileSystem *remote_fs = nullptr;
    FileSystem::IFileSystem *p2pfs = nullptr;
    FileSystem::IFileSystem *cachefs = nullptr;
    FileSystem::IFileSystem *srcfs = nullptr;
    FileSystem::IFileSystem *localfs = nullptr;
    FileSystem::IFileSystem *metafs = nullptr;
    FileSystem::IFileSystem *checkedfs = nullptr;
};

struct ImageFile;

class ImageService {
public:
    ImageService() {}
    ~ImageService();
    int init();
    ImageFile *create_image_file(const char *config_path);
    bool create_dir(const char *dirname);
    bool copy_checksum_file(const char*, const char*);
    void clean_checksum();
    ImageConfigNS::GlobalConfig global_conf;
    struct GlobalFs global_fs;

private:
    int read_global_config_and_set();
    std::pair<std::string, std::string> reload_auth(const char *remote_path);
    void set_result_file(std::string &filename, std::string &data);
    void __do_clean_checksum();
};

ImageService *create_image_service();

int load_cred_from_file(const std::string path, const std::string &remote_path,
                        std::string &username, std::string &password);

void destroy();