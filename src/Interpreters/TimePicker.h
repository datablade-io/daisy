#pragma once

#include <string>

namespace DB
{
using String = std::string;

struct TimePicker
{
private:
    String start;
    String end;

public:
    TimePicker()
    {
        start = "";
        end = "";
    }

    const String & getStart() const { return start; }
    const String & getEnd() const { return end; }

    void setStart(const String & start_) { this->start = start_; }
    void setEnd(const String & end_) { this->end = end_; }
};

}
