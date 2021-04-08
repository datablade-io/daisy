#pragma once

#include <common/types.h>

namespace DB
{
template <typename Clock, typename TimeScale>
inline Int64 clockNow()
{
    return std::chrono::duration_cast<TimeScale>(Clock::now().time_since_epoch()).count();
}

inline Int64 utcNowMilliseconds()
{
    return clockNow<std::chrono::system_clock, std::chrono::milliseconds>();
}

inline Int64 monotonicNowMilliseconds()
{
    return clockNow<std::chrono::steady_clock, std::chrono::milliseconds>();
}
}
