#ifndef REWRITER_LOGGER_HPP
#define REWRITER_LOGGER_HPP

#include <Poco/LocalDateTime.h>
#include <Poco/DateTimeFormatter.h>
#include <ctime>

std::string getFomartTime(const std::string& fmt)
{
    Poco::LocalDateTime now;
    std::string str;
    if(fmt.empty())
    {
        str = Poco::DateTimeFormatter::format(now, "%Y-%m-%d %H:%M:%S.%i");
    }
    else
    {
        str = Poco::DateTimeFormatter::format(now, fmt);
    }

    return str;
}

#define rewriterLogger(msg) \
    do{\
        std::cout << getFomartTime("") << " - " << msg << std::endl; \
    }while(0)

#endif

