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
#include "image_file.h"
#include "image_service.h"
#include "overlaybd/alog.h"
#include "overlaybd/event-loop.h"
#include "overlaybd/fs/filesystem.h"
#include "overlaybd/net/curl.h"
#include "overlaybd/photon/syncio/aio-wrapper.h"
#include "overlaybd/photon/syncio/fd-events.h"
#include "overlaybd/photon/syncio/signal.h"
#include "overlaybd/photon/thread.h"
#include "libtcmu.h"
#include "libtcmu_common.h"
#include <fcntl.h>
#include <sys/resource.h>
#include "obd_device.h"


#define MAX_OPEN_FD 1048576


ImageService *imgservice = nullptr;

class TCMULoop {
protected:
    EventLoop *loop;
    struct tcmulib_context *ctx;
    int fd;

    int wait_for_readable(EventLoop *) {
        auto ret = photon::wait_for_fd_readable(fd);
        if (ret < 0) {
            if (errno == ETIMEDOUT) {
                return 0;
            }
            return -1;
        }
        return 1;
    }

    int on_accept(EventLoop *) {
        tcmulib_master_fd_ready(ctx);
        return 0;
    }

public:
    explicit TCMULoop(struct tcmulib_context *ctx)
        : ctx(ctx), loop(new_event_loop({this, &TCMULoop::wait_for_readable},
                                        {this, &TCMULoop::on_accept})) {
        fd = tcmulib_get_master_fd(ctx);
    }

    ~TCMULoop() {
        loop->stop();
        delete loop;
    }

    void run() { loop->async_run(); }
};

TCMULoop *main_loop = nullptr;

static char *tcmu_get_path(struct tcmu_device *dev) {
    char *config = strchr(tcmu_dev_get_cfgstring(dev), '/');
    if (!config) {
        LOG_ERROR("no configuration found in cfgstring");
        return NULL;
    }
    config += 1;

    return config;
}

static int dev_open(struct tcmu_device *dev) {
    char *config = tcmu_get_path(dev);
    LOG_INFO("dev open `", config);
    if (!config) {
        LOG_ERROR_RETURN(0, -EPERM, "get image config path failed");
    }

    struct timeval start;
    gettimeofday(&start, NULL);

    ImageFile *file = imgservice->create_image_file(config);
    if (file == nullptr) {
        LOG_ERROR_RETURN(0, -EPERM, "create image file failed");
    }

    ObdDevice *odev = new ObdDevice(dev, file, imgservice->global_conf.recycleSec());

    tcmu_dev_set_private(dev, odev);
    tcmu_dev_set_block_size(dev, file->block_size);
    tcmu_dev_set_num_lbas(dev, file->num_lbas);
    tcmu_dev_set_unmap_enabled(dev, true);
    tcmu_dev_set_write_cache_enabled(dev, false);

    struct timeval end;
    gettimeofday(&end, NULL);

    uint64_t elapsed = 1000000UL * (end.tv_sec-start.tv_sec)+ end.tv_usec-start.tv_usec;
    LOG_INFO("dev opened `, time cost ` ms", config, elapsed / 1000);
    return 0;
}

static int close_cnt = 0;
static void dev_close(struct tcmu_device *dev) {
    ObdDevice *odev = (ObdDevice *)tcmu_dev_get_private(dev);
    delete odev;
    close_cnt++;
    malloc_trim(128 * 1024);
    if (close_cnt == 500) {
        malloc_trim(128 * 1024);
        close_cnt = 0;
    }
    LOG_INFO("dev closed");
    return;
}

void sigint_handler(int signal = SIGINT) {
    LOG_INFO("sigint received");
    if (main_loop != nullptr) {
        delete main_loop;
        main_loop = nullptr;
    }
}


int main(int argc, char **argv) {
    mallopt(M_TRIM_THRESHOLD, 128 * 1024);

    photon::init();
    DEFER(photon::fini());
    photon::fd_events_init();
    DEFER(photon::fd_events_fini());
    photon::libaio_wrapper_init();
    DEFER(photon::libaio_wrapper_fini());
    photon::sync_signal_init();
    DEFER(photon::sync_signal_fini());
    Net::libcurl_init();
    DEFER(Net::libcurl_fini());

    photon::block_all_signal();
    photon::sync_signal(SIGTERM, &sigint_handler);
    photon::sync_signal(SIGINT, &sigint_handler);

    imgservice = create_image_service();
    if (imgservice == nullptr) {
        LOG_ERROR("failed to create image service");
        return -1;
    }

    /*
     * Handings for rlimit and netlink are from tcmu-runner main.c
     */
    struct rlimit rlim;
    int ret = getrlimit(RLIMIT_NOFILE, &rlim);
    if (ret == -1) {
        LOG_ERROR("failed to get max open fd limit");
        return ret;
    }
    if (rlim.rlim_max < MAX_OPEN_FD) {
        rlim.rlim_max = MAX_OPEN_FD;
        ret = setrlimit(RLIMIT_NOFILE, &rlim);
        if (ret == -1) {
            LOG_ERROR("failed to set max open fd to [soft: ` hard: `]",
                      (long long int)rlim.rlim_cur,
                      (long long int)rlim.rlim_max);
            return ret;
        }
    }

    /*
     * If this is a restart we need to prevent new nl cmds from being
     * sent to us until we have everything ready.
     */
    LOG_INFO("blocking netlink");
    bool reset_nl_supp = true;
    ret = tcmu_cfgfs_mod_param_set_u32("block_netlink", 1);
    LOG_INFO("blocking netlink done");
    if (ret == -ENOENT) {
        reset_nl_supp = false;
    } else {
        /*
         * If it exists ignore errors and try to reset in case kernel is
         * in an invalid state
         */
        LOG_INFO("resetting netlink");
        tcmu_cfgfs_mod_param_set_u32("reset_netlink", 1);
        LOG_INFO("reset netlink done");
    }

    struct tcmulib_context *tcmulib_ctx;
    struct tcmulib_handler main_handler;
    main_handler.name = "Handler for overlaybd devices";
    main_handler.subtype = "overlaybd";
    main_handler.cfg_desc = "overlaybd bs";
    main_handler.check_config = nullptr;
    main_handler.added = dev_open;
    main_handler.removed = dev_close;

    tcmulib_ctx = tcmulib_initialize(&main_handler, 1);
    if (!tcmulib_ctx) {
        LOG_ERROR("tcmulib init failed.");
        return -1;
    }

    if (reset_nl_supp) {
        tcmu_cfgfs_mod_param_set_u32("block_netlink", 0);
        reset_nl_supp = false;
    }

    main_loop = new TCMULoop(tcmulib_ctx);
    main_loop->run();

    while (main_loop != nullptr) {
        photon::thread_usleep(200*1000);
    }
    LOG_INFO("main loop exited");

    tcmulib_close(tcmulib_ctx);
    LOG_INFO("tcmulib closed");

    // delete imgservice;

    return 0;
}
