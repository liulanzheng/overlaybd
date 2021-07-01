#pragma once

#include <cctype>
#include <string>
#include <sys/stat.h>
#include "overlaybd/fs/filesystem.h"

namespace FileSystem {

/*
 * 1. Prefetcher supports `record` and `replay` operations on a specific container image.
 *    It will persist R/W metadata of all layers into a trace file during container boot-up,
 *    and then replay this file, in order to retrieve data in advance.
 *
 * 2. The trace file will be placed under `prefetch_dir`, and its name should reflect the actual image.
 *    Either image_id or the digest of OverlayBD uppermost layer could be taken into consideration.
 *    After starting recording, a lock file will also be created. Delete it to stop recording.
 *
 * 3. The trace file will be dumped when recording stops. A OK file will be created to indicate dump finished.
 *
 * 4. Work mode:
 *      trace file non-existent         => Disabled
 *      trace file exist but empty      => Start Recording
 *      lock file deleted               => Stop Recording
 *      trace file exist and not empty  => Replay
 */
class Prefetcher : public Object {
public:
    enum class Mode {
        Disabled,
        Record,
        Replay,
    };
    enum class TraceOp : char {
        READ = 'R',
        WRITE = 'W'
    };

    virtual void record(TraceOp op, uint32_t layer_index, size_t count, off_t offset) = 0;

    virtual void replay() = 0;

    // Prefetch file inherits ForwardFile, and it is the actual caller of `record` method.
    // The source file is supposed to have cache.
    virtual IFile* new_prefetch_file(IFile* src_file, uint32_t layer_index) = 0;

    static Mode detect_mode(const std::string& trace_file_path, size_t* file_size = nullptr) {
        struct stat buf = {};
        int ret = stat(trace_file_path.c_str(), &buf);
        if (file_size != nullptr) {
            *file_size = buf.st_size;
        }
        if (ret != 0) {
            return Mode::Disabled;
        } else if (buf.st_size == 0) {
            return Mode::Record;
        } else {
            return Mode::Replay;
        }
    }

    Mode get_mode() const {
        return m_mode;
    }

protected:
    Mode m_mode;
};

Prefetcher* new_prefetcher(const std::string& prefetch_dir, const std::string& trace_file_name);

Prefetcher* new_prefetcher_v2(const std::string& trace_file_path);

}