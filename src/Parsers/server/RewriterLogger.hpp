#ifndef REWRITER_LOGGER_HPP
#define REWRITER_LOGGER_HPP

//#include <Poco/Logger.h>
//#include <Poco/PatternFormatter.h>
//#include <Poco/FormattingChannel.h>
//#include <Poco/ConsoleChannel.h>
//#include "Poco/Message.h"
//
//#include <iostream>
//
//class RewriterLogger
//{
//    private:
//        Poco::FormattingChannel* pFC;
//        Poco::Logger *logger;
//
//    public:
//        RewriterLogger(const std::string& fmt)
//        {
//            initLogger(fmt);
//        }
//
//        void initLogger(const std::string & fmt)
//        {
//            std::cout<< "new RewriterLogger" << std::endl;
//            pFC = new Poco::FormattingChannel(new Poco::PatternFormatter(fmt));
//            pFC->setChannel(new Poco::ConsoleChannel);
//            pFC->open();
//            logger = &Poco::Logger::create("ConsoleLogger", pFC, Poco::Message::PRIO_INFORMATION);
//        }
//
//        void log(const std::string& msg)
//        {
//            if(nullptr == logger)
//            {
//                initLogger("%Y-%m-%d %H:%M:%S.%c %t");
//            }
//            std::cout << "logger address : " << this << "; ";
//            //std::cout << "function address : " << logger -> log << " : ";
//            logger -> log(msg);
//        }
//};
//
//RewriterLogger rewriterLogger("%Y-%m-%d %H:%M:%S.%c %t");

#include <Poco/LocalDateTime.h>
#include <Poco/DateTimeFormatter.h>
#include <ctime>

std::string getFomartTime(const std::string& fmt)
{
    Poco::LocalDateTime now;
    std::string str;
    if(fmt.empty())
    {
        str = Poco::DateTimeFormatter::format(now, "%Y-%m-%d %H:%M:%S.%c");
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

