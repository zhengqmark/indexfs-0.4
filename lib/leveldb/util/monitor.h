#ifndef MONITOR_H_
#define MONITOR_H_
#include <string>
#include <map>
#include <vector>
#include <time.h>
#include <stdio.h>

namespace leveldb {

typedef time_t TTSTime;
typedef long long TTSValue;
typedef std::pair<TTSTime, TTSValue> TSEntry;
typedef std::vector<TSEntry> TSeries;
typedef std::map<std::string, TSeries> TMetList;

class MetricStat {
public:
  virtual ~MetricStat() {
  }

  void AddMetric(TMetList &metlist, std::string name,
                 TTSValue ts, TTSValue val) {
    TMetList::iterator it = metlist.find(name);
    if (it == metlist.end()) {
      metlist[name] = TSeries();
      it = metlist.find(name);
    }
    it->second.push_back(TSEntry(ts, val));
  }

  virtual void GetMetric(TMetList &metlist, time_t now) = 0;
};

class Monitor {
protected:
  TMetList metlist;
  std::vector<MetricStat*> statlist;
  FILE *logfile;

public:
  Monitor();

  Monitor(const std::string &part);

  Monitor(const std::string &part,
          const std::string &fs);

  ~Monitor();

  void SetLogFile(const std::string logfilename);

  void DoMonitor();

  void AddMetricStat(MetricStat* mstat);

  void AddMetric(std::string name, TTSValue ts, TTSValue val);

  void Report();

  void Report(FILE *logf);

  void ReportToFile();
};

}

#endif
