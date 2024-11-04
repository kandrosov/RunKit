import os
import ROOT

class EventFilter:
  singleton = None
  cpp_code = """

class EventFilter {
public:
  using IdType = std::tuple<unsigned int, unsigned int, unsigned long long>;
  using EventSet = std::set<IdType>;

  static bool addEvent(unsigned int run, unsigned int lumi, unsigned long long event, bool verbose = false) {
    const std::lock_guard<std::mutex> lock(getMutex());
    auto& events = getEventSet();
    IdType id(run, lumi, event);
    if(events.find(id) != events.end()) {
      if(verbose)
        std::cout << "Duplicate event: run:lumi:event = " << run << ":" << lumi << ":" << event << std::endl;
      return false;
    }
    events.insert(id);
    return true;
  }

  static size_t size() {
    const std::lock_guard<std::mutex> lock(getMutex());
    return getEventSet().size();
  }

  static void makeSnapshot(const std::string& file_name) {
    const std::lock_guard<std::mutex> lock(getMutex());
    const auto& events = getEventSet();
    auto event_iter = events.begin();
    std::mutex next_mutex;
    auto get_next = [&]() {
      const std::lock_guard<std::mutex> lock(next_mutex);
      if(event_iter == events.end())
        throw std::out_of_range("Event iterator out of range");
      return *event_iter++;
    };

    ROOT::RDataFrame df_base(events.size());
    auto df = df_base.Define("entry", get_next);
    df = df.Define("run", "std::get<0>(entry)");
    df = df.Define("luminosityBlock", "std::get<1>(entry)");
    df = df.Define("event", "std::get<2>(entry)");

    ROOT::RDF::RSnapshotOptions opt;
    opt.fMode = "RECREATE";
    opt.fCompressionAlgorithm = ROOT::kLZMA;
    opt.fCompressionLevel = 9;

    df.Snapshot("Events", file_name, {"run", "luminosityBlock", "event"}, opt);
  }

private:
  static EventSet& getEventSet() {
    static EventSet eventSet;
    return eventSet;
  }

  static std::mutex& getMutex() {
    static std::mutex mutex;
    return mutex;
  }
};
"""
  def __init__(self, prev_output, current_output):
    if EventFilter.singleton:
      raise Exception("EventFilter is a singleton")
    if not prev_output:
      raise Exception("Previous output is required")
    if not current_output:
      raise Exception("Current output is required")

    self.prev_output = prev_output
    self.current_output = current_output

    ROOT.gInterpreter.Declare(EventFilter.cpp_code)
    if os.path.exists(prev_output):
      df_prev = ROOT.RDataFrame("Events", prev_output)
      n_prev = df_prev.Filter("EventFilter::addEvent(run, luminosityBlock, event)").Count().GetValue()
      print(f"Loaded {n_prev} event IDs from {prev_output}")

  def filter(self, df):
    print(f"EventFilter::size = {ROOT.EventFilter.size()}")
    return df.Filter("EventFilter::addEvent(run, luminosityBlock, event)")

  def createSnapshot(self):
    ROOT.EventFilter.makeSnapshot(self.current_output)

  @staticmethod
  def getSingleton(prev_output=None, current_output=None):
    if not EventFilter.singleton:
      EventFilter.singleton = EventFilter(prev_output, current_output)
    return EventFilter.singleton

def filter(df, prev_output, current_output):
  return EventFilter.getSingleton(prev_output, current_output).filter(df)

def OnSkimFinish():
  print(f"Creating snapshot of processed event IDs (total ids = {ROOT.EventFilter.size()}) ...")
  EventFilter.getSingleton().createSnapshot()
  print("ID snapshot created")