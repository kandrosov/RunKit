import ROOT

initialized = False

def filter(df):
    global initialized
    if not initialized:
        ROOT.gInterpreter.Declare("""
            using lumiEventMapType = std::map<unsigned int, std::set<unsigned long long>>;
            using eventMapType =  std::map<unsigned int, lumiEventMapType>;
            bool saveEvent(unsigned int run, unsigned int lumi, unsigned long long event) {
                static eventMapType eventMap;
                static std::mutex eventMap_mutex;
                const std::lock_guard<std::mutex> lock(eventMap_mutex);
                auto& events = eventMap[run][lumi];
                if(events.find(event) != events.end())
                    return false;
                events.insert(event);
                return true;
            }
        """)
        initialized = True

    return df.Filter("saveEvent(run, luminosityBlock, event)")