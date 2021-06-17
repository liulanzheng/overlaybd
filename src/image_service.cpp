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
#include "overlaybd/alog-stdstring.h"
#include "overlaybd/alog.h"
#include "overlaybd/base64.h"
#include "overlaybd/fs/cache/cache.h"
#include "overlaybd/fs/filesystem.h"
#include "overlaybd/fs/localfs.h"
#include "overlaybd/fs/registryfs/registryfs.h"
#include "overlaybd/fs/checkedfs/checkedfs.h"
#include "overlaybd/fs/tar_file.h"
#include "overlaybd/fs/zfile/zfile.h"
#include "overlaybd/fs/p2pfs/root_selector.h"
#include "overlaybd/fs/p2pfs/p2pfs.h"
#include "overlaybd/net/socket.h"
#include "overlaybd/photon/thread.h"
#include <dirent.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <string>
#include <sys/types.h>
#include <unistd.h>
#include <vector>

const std::string DEFAULT_CONFIG_PATH = "/etc/overlaybd/overlaybd.json";
const std::string DEFAULT_CHECKSUM_PATH = "/opt/overlaybd/checksum";
const int LOG_SIZE_MB = 10;
const int LOG_NUM = 3;

struct ImageRef {
    std::vector<std::string> seg; // cr: seg_0, ns: seg_1, repo: seg_2
};

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
            if (words.size() != 5) {
                LOG_ERROR_RETURN(0, -1, "invalid blob url: `", url);
            }
            ref.seg = std::vector<std::string>{words[0], words[2], words[3]};
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

bool ImageService::create_dir(const char *dirname) {
    if (global_fs.localfs->access(dirname, 0) == 0) {
        return true;
    }
    if (global_fs.localfs->mkdir(dirname, 0644) == 0) {
        LOG_INFO("dir ` doesn't exist. create succ.", dirname);
        return true;
    }
    LOG_ERRNO_RETURN(0, false, "dir ` doesn't exist. create failed.", dirname);
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

    LOG_INFO("global config: cache_dir: `, cache_size_GB: `",
             global_conf.registryCacheDir(), global_conf.registryCacheSizeGB());

    if (global_conf.enableAudit()) {
        std::string auditPath = global_conf.auditPath();
        if (auditPath == "") {
            LOG_WARN("empty audit path, ignore audit");
        } else {
            LOG_INFO("set audit_path:`", global_conf.auditPath());
            default_audit_logger.log_output = new_log_output_file(global_conf.auditPath().c_str(), LOG_SIZE_MB, LOG_NUM);
        }
    } else {
        LOG_INFO("audit disabled");
    }

    set_log_output_level(global_conf.logLevel());
    LOG_INFO("set log_level:`", global_conf.logLevel());

    if (global_conf.logPath() != "") {
        LOG_INFO("set log_path:`", global_conf.logPath());
        int ret = log_output_file(global_conf.logPath().c_str(), LOG_SIZE_MB, LOG_NUM);
        if (ret != 0) {
            LOG_ERROR_RETURN(0, -1, "log_output_file failed, errno:`", errno);
        }
    }
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

    auto file = FileSystem::open_localfile_adaptor(filename.c_str(),
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

static std::string meta_name_trans(const char *fn) {
    std::string ret(fn);
    std::string kw = "/sha256:";
    auto p = ret.find(kw);
    if (p == std::string::npos) {
        return ret;
    }
    LOG_DEBUG("` -> `", fn, ret.substr(p));
    return ret.substr(p);
}

int ImageService::init() {
    if (read_global_config_and_set() < 0) {
        return -1;
    }

    if (global_fs.localfs == nullptr) {
        global_fs.localfs = FileSystem::new_localfs_adaptor(nullptr, 0);
    }

    if (create_dir(global_conf.registryCacheDir().c_str()) == false)
        return -1;
    if (create_dir(DEFAULT_CHECKSUM_PATH.c_str()) == false)
        return -1;

    if (global_fs.p2p_fs == nullptr && global_conf.p2p().enable()) {
        //TODO
        // auto rs = FileSystem::new_single_root_selector(
        //     FileSystem::NodeID(Net::EndPoint{Net::IPAddr(global_conf.p2p().ip()), global_conf.p2p().port()}));
        // if (rs == nullptr) {
        //     LOG_ERROR_RETURN(0, -1,
        //         "new_single_root_selector failed. ip: `, port: `",
        //         global_conf.p2p().ip(), global_conf.p2p().port());
        // }
        // global_fs.p2p_fs = FileSystem::new_p2pfs(rs, FileSystem::NodeID(), nullptr, nullptr, false, 200, 3,
        //         nullptr, 1000UL * 1000, 1000000UL * global_conf.p2p().timeout());
    }

    if (global_fs.remote_fs == nullptr) {
        auto cafile = "/etc/ssl/certs/ca-bundle.crt";
        if (access(cafile, 0) != 0) {
            cafile = "/etc/ssl/certs/ca-certificates.crt";
            if (access(cafile, 0) != 0) {
                LOG_ERROR_RETURN(0, -1, "no certificates found.");
            }
        }

        LOG_INFO("create registryfs with cafile:`", cafile);
        auto registry_fs = FileSystem::new_registryfs_with_credential_callback(
            {this, &ImageService::reload_auth}, cafile, 30UL * 1000000);
        if (registry_fs == nullptr) {
            LOG_ERROR_RETURN(0, -1, "create registryfs failed.");
        }

        auto metafs = FileSystem::new_localfs_adaptor(DEFAULT_CHECKSUM_PATH.c_str());
        LOG_INFO("create checkedfs, checksum path: `", DEFAULT_CHECKSUM_PATH);
        auto checkedfs = FileSystem::new_checkedfs_adaptor_v1(registry_fs, metafs, meta_name_trans);

        auto registry_cache_fs = FileSystem::new_localfs_adaptor(
            global_conf.registryCacheDir().c_str());
        if (registry_cache_fs == nullptr) {
            delete registry_fs;
            delete checkedfs;
            LOG_ERROR_RETURN(0, -1, "new_localfs_adaptor for ` failed",
                             global_conf.registryCacheDir().c_str());
            return false;
        }

        LOG_INFO("create cache with size: ` GB",
                 global_conf.registryCacheSizeGB());
        global_fs.remote_fs = FileSystem::new_full_file_cached_fs(
            checkedfs, registry_cache_fs, 256 * 1024 /* refill unit 256KB */,
            global_conf.registryCacheSizeGB() /*GB*/, 10000000,
            (uint64_t)1048576 * 4096, nullptr);

        if (global_fs.remote_fs == nullptr) {
            delete registry_fs;
            delete checkedfs;
            delete registry_cache_fs;
            LOG_ERROR_RETURN(0, -1,
                             "create remotefs (registryfs + cache) failed.");
        }
        global_fs.cachefs = registry_cache_fs;
        global_fs.srcfs = registry_fs;
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
        LOG_INFO("rollback delete image file");
        delete ret;
        return NULL;
    }
    std::string data = "success";
    set_result_file(resFile, data);
    return ret;
}
void ImageService::__do_clean_checksum() {
    auto dirp = global_fs.localfs->opendir(DEFAULT_CHECKSUM_PATH.c_str());
    if (dirp == nullptr) {
        LOG_ERROR("open checksumdir ` failed.", dirp);
        return;
    }
    DEFER(global_fs.localfs->closedir(dirp));
    auto ent = global_fs.localfs->readdir(dirp);
    while (ent != nullptr) {
        std::string basename = ent->d_name;
        if (basename == "." || basename == "..") {
            ent = global_fs.localfs->readdir(dirp);
            continue;
        }
        auto fullpath = DEFAULT_CHECKSUM_PATH + "/" + basename;
        auto touch = global_fs.localfs->access(fullpath.c_str() , F_OK);
        LOG_DEBUG("check ` is valid: `", basename, touch == 0);
        if (touch != 0) {
            LOG_INFO("remove invalid symbol link: `", fullpath);
            global_fs.localfs->unlink(fullpath.c_str());
        }
        ent = global_fs.localfs->readdir(dirp);
    }
    LOG_INFO("clean checksum done.");
}

void ImageService::clean_checksum() {
    photon::thread_create11(&ImageService::__do_clean_checksum, this);
}

bool ImageService::copy_checksum_file(const char* src, const char* dst_basename) {
    std::string dst = DEFAULT_CHECKSUM_PATH + "/" + dst_basename;
    auto touch = global_fs.localfs->access(dst.c_str(), F_OK);
    if (touch == 0) {
        LOG_DEBUG("checksum file ` already exists.", dst);
        return true;
    }
    LOG_INFO("try to unlink old symbol link: `, ret: `",
                    dst, global_fs.localfs->unlink(dst.c_str()));

    if (global_fs.localfs->symlink(src, dst.c_str()) != 0) {
        LOG_ERRNO_RETURN(0, -1, "create symbol link failed: ` -> `", src, dst.c_str());
    }
    return true;
}


ImageService *create_image_service() {
    ImageService *ret = new ImageService();
    if (ret->init() < 0) {
        delete ret;
        return nullptr;
    }
    return ret;
}

namespace FileSystem {
    RefFile* get_ref_file(const std::string &key) {
        auto it = opened.find(key);
        if (it != opened.end()) {
            it->second->ref_count++;
            LOG_INFO("return shared file `", key);
            return new RefFile(it->second);
        }
        return nullptr;
    }

    RefFile* new_ref_file(IFile *file, const std::string &key) {
        auto found = get_ref_file(key);
        if (found != nullptr)
            return found;
        return new RefFile(new RefObj(file, key));
    }
}
