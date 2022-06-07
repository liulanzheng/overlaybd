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
#include <vector>
#include "config_util.h"

namespace ImageConfigNS {
const int MAX_LAYER_CNT = 256;

struct LayerConfig : public ConfigUtils::Config {
    APPCFG_CLASS;

    APPCFG_PARA(file, std::string, "");
    APPCFG_PARA(dir, std::string, "");
    APPCFG_PARA(digest, std::string, "");
    APPCFG_PARA(size, uint64_t, 0);
};

struct UpperConfig : public ConfigUtils::Config {
    APPCFG_CLASS;

    APPCFG_PARA(index, std::string, "");
    APPCFG_PARA(data, std::string, "");
};

struct DownloadConfig : public ConfigUtils::Config {
    APPCFG_CLASS;

    APPCFG_PARA(enable, bool, false);
    APPCFG_PARA(delay, int, 300);
    APPCFG_PARA(delayExtra, int, 30);
    APPCFG_PARA(maxMBps, int, 100);
    APPCFG_PARA(tryCnt, int, 5);
};

struct ImageConfig : public ConfigUtils::Config {
    APPCFG_CLASS;

    APPCFG_PARA(repoBlobUrl, std::string, "");
    APPCFG_PARA(lowers, std::vector<LayerConfig>);
    APPCFG_PARA(upper, UpperConfig);
    APPCFG_PARA(resultFile, std::string, "");
    APPCFG_PARA(download, DownloadConfig);
    APPCFG_PARA(accelerationLayer, bool, false);
    APPCFG_PARA(recordTracePath, std::string, "");
};

struct GlobalConfig : public ConfigUtils::Config {
    APPCFG_CLASS

    APPCFG_PARA(registryCacheDir, std::string, "/opt/overlaybd/registryfs_cache");
    APPCFG_PARA(credentialFilePath, std::string, "/opt/overlaybd/cred.json");
    APPCFG_PARA(registryCacheSizeGB, uint32_t, 4);
    APPCFG_PARA(ioEngine, uint32_t, 0);
    APPCFG_PARA(cacheType, std::string, "file");
    APPCFG_PARA(logLevel, uint32_t, 1);
    APPCFG_PARA(logPath, std::string, "/var/log/overlaybd.log");
    APPCFG_PARA(download, DownloadConfig);
    APPCFG_PARA(enableAudit, bool, true);
    APPCFG_PARA(auditPath, std::string, "/var/log/overlaybd-audit.log");
};

struct AuthConfig : public ConfigUtils::Config {
    APPCFG_CLASS

    APPCFG_PARA(auths, ConfigUtils::Document);
};

} // namespace ImageConfigNS