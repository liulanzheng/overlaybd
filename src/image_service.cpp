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
#include "image_service.h"
#include "config.h"
#include "image_file.h"
#include <photon/common/alog-stdstring.h>
#include <photon/common/alog.h>
#include <photon/common/io-alloc.h>
#include <photon/fs/localfs.h>
#include <photon/thread/thread.h>
#include "overlaybd/cache/cache.h"
#include "overlaybd/registryfs/registryfs.h"
#include "overlaybd/tar_file.h"
#include "overlaybd/zfile/zfile.h"
#include "overlaybd/base64.h"
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <string>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

const char *DEFAULT_CONFIG_PATH = "/etc/overlaybd/overlaybd.json";
const int LOG_SIZE = 10 * 1024 * 1024;
const int LOG_NUM = 3;

struct ImageRef {
    std::vector<std::string> seg; // cr: seg_0, ns: seg_1, repo: seg_2, seg_3,...
};

bool create_dir(const char *dirname) {
    auto lfs = new_localfs_adaptor();
    if (lfs == nullptr) {
        LOG_ERRNO_RETURN(0, false, "new localfs_adaptor failed");
    }
    DEFER(delete lfs);
    if (lfs->access(dirname, 0) == 0) {
        return true;
    }
    if (lfs->mkdir(dirname, 0644) == 0) {
        LOG_INFO("dir ` doesn't exist. create succ.", dirname);
        return true;
    }
    LOG_ERRNO_RETURN(0, false, "dir ` doesn't exist. create failed.", dirname);
}

int parse_blob_url(const std::string &url, struct ImageRef &ref) {
    for (auto prefix : std::vector<std::string>{"http://", "https://"}) {
        auto p_colon = url.find(prefix);
        if (p_colon == 0) {
            auto sub_url = url.substr(prefix.length());
            std::vector<std::string> words;
            size_t idx = 0, prev = 0;
            while ((idx = sub_url.find("/", prev)) != std::string::npos) {
                LOG_DEBUG("find: `", sub_url.substr(prev, idx - prev));
                words.emplace_back(sub_url.substr(prev, idx - prev));
                prev = idx + 1;
            }
            ref.seg = std::vector<std::string>{words[0]};
            for (int i = 2; i + 1 < words.size(); i++) {
                ref.seg.push_back(words[i]);
            }
        }
    }
    return 0;
}

int load_cred_from_file(const std::string path, const std::string &remote_path,
                        std::string &username, std::string &password) {
    ImageConfigNS::AuthConfig cfg;
    if (!cfg.ParseJSON(path)) {
        LOG_ERROR_RETURN(0, -1, "parse json failed: `", path);
    }

    struct ImageRef ref;
    if (parse_blob_url(remote_path, ref) != 0) {
        LOG_ERROR_RETURN(0, -1, "parse blob url failed: `", remote_path);
    }

    for (auto &iter : cfg.auths().GetObject()) {
        std::string addr = iter.name.GetString();
        LOG_DEBUG("cred addr: `", iter.name.GetString());

        std::string prefix = "";
        bool match = false;
        for (auto seg : ref.seg) {
            if (prefix != "")
                prefix += "/";
            prefix += seg;
            if (addr == prefix) {
                match = true;
                break;
            }
        }

        if (!match)
            continue;

        if (iter.value.HasMember("auth")) {
            auto token = base64_decode(iter.value["auth"].GetString());
            auto p = token.find(":");
            if (p == std::string::npos) {
                LOG_ERROR("invalid base64 auth, no ':' found: `", token);
                continue;
            }
            username = token.substr(0, p);
            password = token.substr(p + 1);
            return 0;
        } else if (iter.value.HasMember("username") &&
                   iter.value.HasMember("password")) {
            username = iter.value["username"].GetString();
            password = iter.value["password"].GetString();
            return 0;
        }
    }

    return -1;
}

int ImageService::read_global_config_and_set() {
    if (!global_conf.ParseJSON(DEFAULT_CONFIG_PATH)) {
        LOG_ERROR_RETURN(0, -1, "error parse global config json: `",
                         DEFAULT_CONFIG_PATH);
    }
    uint32_t ioengine = global_conf.ioEngine();
    if (ioengine > 2) {
        LOG_ERROR_RETURN(0, -1, "unknown io_engine: `", ioengine);
    }
    if (global_conf.cacheType() != "file" && global_conf.cacheType() != "ocf") {
        LOG_ERROR_RETURN(0, -1, "unknown cache type `", global_conf.cacheType());
    }

    if (global_conf.enableAudit()) {
        std::string auditPath = global_conf.auditPath();
        if (auditPath == "") {
            LOG_WARN("empty audit path, ignore audit");
        } else {
            LOG_INFO("set audit_path:`", global_conf.auditPath());
            default_audit_logger.log_output = new_log_output_file(global_conf.auditPath().c_str(), LOG_SIZE, LOG_NUM);
        }
    } else {
        LOG_INFO("audit disabled");
    }

    set_log_output_level(global_conf.logLevel());
    LOG_INFO("set log_level:`", global_conf.logLevel());

    if (global_conf.logPath() != "") {
        LOG_INFO("set log_path:`", global_conf.logPath());
        int ret = log_output_file(global_conf.logPath().c_str(), LOG_SIZE, LOG_NUM);
        if (ret != 0) {
            LOG_ERROR_RETURN(0, -1, "log_output_file failed, errno:`", errno);
        }
    }

    LOG_INFO("global config: cache_dir: `, cache_size_GB: `, cache_type `",
             global_conf.registryCacheDir(), global_conf.registryCacheSizeGB(), global_conf.cacheType());
    return 0;
}

std::pair<std::string, std::string>
ImageService::reload_auth(const char *remote_path) {
    LOG_DEBUG("Acquire credential for ", VALUE(remote_path));
    std::string username, password;

    int res = load_cred_from_file(global_conf.credentialFilePath(), std::string(remote_path), username, password);
    if (res == 0) {
        LOG_INFO("auth found for `: `", remote_path, username);
        return std::make_pair(username, password);
    }
    return std::make_pair("", "");
}

void ImageService::set_result_file(std::string &filename, std::string &data) {
    if (filename == "") {
        LOG_WARN("no resultFile config set, ignore writing result");
        return;
    }

    auto file = open_localfile_adaptor(filename.c_str(),
                                                   O_RDWR | O_CREAT | O_TRUNC);
    if (file == nullptr) {
        LOG_ERROR("failed to open result file", filename);
        return;
    }
    DEFER(delete file);

    if (file->write(data.c_str(), data.size()) != (ssize_t)data.size()) {
        LOG_ERROR("write(`,`), path:`, `:`", data.c_str(), data.size(),
                  filename.c_str(), errno, strerror(errno));
    }
    LOG_DEBUG("write to result file: `, content: `", filename.c_str(),
              data.c_str());
}

int ImageService::init() {
    if (read_global_config_and_set() < 0) {
        return -1;
    }

    if (!create_dir(global_conf.registryCacheDir().c_str()))
        return -1;

    if (global_fs.remote_fs == nullptr) {
        auto cafile = "/etc/ssl/certs/ca-bundle.crt";
        if (access(cafile, 0) != 0) {
            cafile = "/etc/ssl/certs/ca-certificates.crt";
            if (access(cafile, 0) != 0) {
                LOG_ERROR_RETURN(0, -1, "no certificates found.");
            }
        }

        LOG_INFO("create registryfs with cafile:`", cafile);
        auto registry_fs = new_registryfs_with_credential_callback(
            {this, &ImageService::reload_auth}, cafile, 30UL * 1000000);
        if (registry_fs == nullptr) {
            LOG_ERROR_RETURN(0, -1, "create registryfs failed.");
        }

        auto tar_fs = new_tar_fs_adaptor(registry_fs);
        if (tar_fs == nullptr) {
            delete registry_fs;
            LOG_ERROR_RETURN(0, -1, "create tar_fs failed.");
        }
        global_fs.srcfs = tar_fs;

        if (global_conf.cacheType() == "file") {
            auto registry_cache_fs = new_localfs_adaptor(
                global_conf.registryCacheDir().c_str());
            if (registry_cache_fs == nullptr) {
                delete tar_fs;
                LOG_ERROR_RETURN(0, -1, "new_localfs_adaptor for ` failed",
                                 global_conf.registryCacheDir().c_str());
            }
            // file cache will delete its src_fs automatically when destructed
            global_fs.remote_fs = FileSystem::new_full_file_cached_fs(
                tar_fs, registry_cache_fs, 256 * 1024 /* refill unit 256KB */,
                global_conf.registryCacheSizeGB() /*GB*/, 10000000,
                (uint64_t)1048576 * 4096, nullptr);

        } else if (global_conf.cacheType() == "ocf") {
            auto namespace_dir = std::string(global_conf.registryCacheDir() + "/namespace");
            if (::access(namespace_dir.c_str(), F_OK) != 0 && ::mkdir(namespace_dir.c_str(), 0755) != 0) {
                LOG_ERRNO_RETURN(0, -1, "failed to create namespace_dir");
            }
            auto namespace_fs = new_localfs_adaptor(namespace_dir.c_str());
            if (namespace_fs == nullptr) {
                LOG_ERROR_RETURN(0, -1, "failed tp create namespace_fs");
            }
            global_fs.namespace_fs = namespace_fs;

            auto io_alloc = new IOAlloc;
            global_fs.io_alloc = io_alloc;

            bool reload_media;
            IFile* media_file;
            auto media_file_path = std::string(global_conf.registryCacheDir() + "/cache_media");
            if (::access(media_file_path.c_str(), F_OK) != 0) {
                reload_media = false;
                media_file = open_localfile_adaptor(media_file_path.c_str(), O_RDWR | O_CREAT, 0644,
                                                                ioengine_psync);
                media_file->fallocate(0, 0, global_conf.registryCacheSizeGB() * 1024UL * 1024 * 1024);
            } else {
                reload_media = true;
                media_file = open_localfile_adaptor(media_file_path.c_str(), O_RDWR, 0644,
                                                                ioengine_psync);
            }
            global_fs.media_file = media_file;

            global_fs.remote_fs = FileSystem::new_ocf_cached_fs(tar_fs, namespace_fs, 65536, 0,
                                                                media_file, reload_media, io_alloc);
        }

        if (global_fs.remote_fs == nullptr) {
            LOG_ERROR_RETURN(0, -1, "create remotefs (registryfs + cache) failed.");
        }
    }
    return 0;
}

ImageFile *ImageService::create_image_file(const char *config_path) {
    ImageConfigNS::GlobalConfig defaultDlCfg;
    if (!defaultDlCfg.ParseJSON(DEFAULT_CONFIG_PATH)) {
        LOG_WARN("default download config parse failed, ignore");
    }
    ImageConfigNS::ImageConfig cfg;
    if (!cfg.ParseJSON(config_path)) {
        LOG_ERROR_RETURN(0, nullptr, "error parse image config");
    }

    if (!cfg.HasMember("download") && !defaultDlCfg.IsNull() &&
        defaultDlCfg.HasMember("download")) {
        cfg.AddMember("download", defaultDlCfg["download"], cfg.GetAllocator());
    }

    auto resFile = cfg.resultFile();
    ImageFile *ret = new ImageFile(cfg, *this);
    if (ret->m_status <= 0) {
        std::string data = "failed:" + ret->m_exception;
        set_result_file(resFile, data);
        delete ret;
        return NULL;
    }
    std::string data = "success";
    set_result_file(resFile, data);
    return ret;
}

ImageService::~ImageService() {
    delete global_fs.remote_fs;
    delete global_fs.media_file;
    delete global_fs.namespace_fs;
    delete global_fs.srcfs;
    delete global_fs.io_alloc;
    LOG_INFO("image service is fully stopped");
}

ImageService *create_image_service() {
    ImageService *ret = new ImageService();
    if (ret->init() < 0) {
        delete ret;
        return nullptr;
    }
    return ret;
}