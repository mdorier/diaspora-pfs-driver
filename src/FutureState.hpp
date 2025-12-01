#ifndef DIASPORA_PFS_DRIVER_FUTURE_STATE_H
#define DIASPORA_PFS_DRIVER_FUTURE_STATE_H

#include <diaspora/Exception.hpp>
#include <mutex>
#include <condition_variable>
#include <variant>

namespace pfs {

template<typename T>
struct FutureState {

    std::mutex                           mutex;
    std::condition_variable              cv;
    std::variant<T, diaspora::Exception> value;
    bool                                 is_set = false;

    template<typename U>
    void set(U u) {
        if(is_set) throw diaspora::Exception{"Promise already set"};
        {
            std::unique_lock lock{mutex};
            value = std::move(u);
            is_set = true;
        }
        cv.notify_all();
    }

    T wait(int timeout_ms) {
        std::unique_lock lock{mutex};
        if(is_set) {
            if(std::holds_alternative<T>(value))
                return std::get<T>(value);
            else
                throw std::get<diaspora::Exception>(value);
        }
        if(timeout_ms > 0)
            cv.wait_for(lock, std::chrono::milliseconds{timeout_ms});
        else
            while(!is_set) cv.wait(lock);
        if(std::holds_alternative<T>(value))
            return std::get<T>(value);
        else
            throw std::get<diaspora::Exception>(value);
    }

    bool test() {
        std::unique_lock lock{mutex};
        return is_set;
    }
};

}

#endif
