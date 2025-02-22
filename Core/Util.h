/* Copyright (c) 2011-2012 Stanford University
 * Copyright (c) 2015 Diego Ongaro
 *
 * Permission to use, copy, modify, and distribute this software for any
 * purpose with or without fee is hereby granted, provided that the above
 * copyright notice and this permission notice appear in all copies.
 *
 * THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR(S) DISCLAIM ALL WARRANTIES
 * WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
 * MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL AUTHORS BE LIABLE FOR
 * ANY SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES
 * WHATSOEVER RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN
 * ACTION OF CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF
 * OR IN CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.
 */

/**
 * \file
 * Common utilities and definitions.
 * See also Core::STLUtil and Core::StringUtil.
 */

#include <cassert>
#include <cinttypes>
#include <condition_variable>
#include <functional>
#include <mutex>
#include <queue>
#include <stdexcept>

#ifndef LOGCABIN_CORE_UTIL_H
#define LOGCABIN_CORE_UTIL_H

namespace LogCabin {
namespace Core {
namespace Util {

/**
 * Cast a bigger int down to a smaller one.
 * Asserts that no precision is lost at runtime.
 */
// This was taken from the RAMCloud project.
template<typename Small, typename Large>
Small
downCast(const Large& large)
{
    Small small = static_cast<Small>(large);
    // The following comparison (rather than "large==small") allows
    // this method to convert between signed and unsigned values.
    assert(large - static_cast<Large>(small) == 0);
    return small;
}

/// Like sizeof but returns a uint32_t.
#define sizeof32(x) LogCabin::Core::Util::downCast<uint32_t>(sizeof(x))

/**
 * Calls a function when this object goes out of scope.
 * This is useful for deferring execution of something until the end of the
 * scope without creating a full-blown RAII class to wrap it. It's named after
 * the try { .. } finally { ... } control structure found in many languages.
 * See also http://www.stroustrup.com/bs_faq2.html#finally
 */
class Finally {
  public:
    explicit Finally(std::function<void()> onDestroy)
        : onDestroy(onDestroy) {
    }
    ~Finally() {
        onDestroy();
    }
    std::function<void()> onDestroy;
};

/**
 * Return true if the log of x in base 2 is a whole number.
 */
bool
isPowerOfTwo(uint64_t x);

/**
 * Copy some noncontiguous chunks of data into a contiguous chunk.
 * \param dest
 *      Where the new copy should be written.
 * \param src
 *      A list of (pointer, length) pairs describing where to copy from.
 * \return
 *      dest (as in memcpy).
 */
void*
memcpy(void* dest,
       std::initializer_list<std::pair<const void*, size_t>> src);

/**
 * The thread could not complete its task because it was asked to exit.
 */
class ThreadInterruptedException : public std::runtime_error {
  public:
    ThreadInterruptedException();
};

template <typename T>
class ThreadSafeQueue {
private:
    std::queue<T> queue_;
    mutable std::mutex mutex_;
    std::condition_variable cv_;

public:
    ThreadSafeQueue() : queue_(), mutex_(), cv_() {}

    // Push an item into the queue
    void push(T value) {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push(std::move(value));
        cv_.notify_all();
    }

    // Try to pop an item with a timeout
    bool pop(T& value, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (!cv_.wait_for(lock, timeout, [this] { return !queue_.empty(); })) {
            return false;
        }

        value = std::move(queue_.front());
        queue_.pop();
        return true;
    }
    
    size_t size() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return queue_.size();
    }
};

} // namespace LogCabin::Core::Util
} // namespace LogCabin::Core
} // namespace LogCabin

#endif /* LOGCABIN_CORE_UTIL_H */
