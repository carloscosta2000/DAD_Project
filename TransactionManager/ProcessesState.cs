using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TransactionManager
{
    public class ProcessesState
    {
        public readonly int DEADLINE = 5;
        private Dictionary<string, bool> statesSuspects;

        public void initDataStructure(List<string> nodes)
        {
            statesSuspects = new Dictionary<string, bool>();
            foreach (string id in nodes)
                statesSuspects[id] = false;
        }

        public void suspectProcess(string id)
        {
            if(statesSuspects != null && statesSuspects.ContainsKey(id))
                statesSuspects[id] = true;
        }

        public void stopSuspectingProcess(string id)
        {
            if (statesSuspects != null && statesSuspects.ContainsKey(id))
                statesSuspects[id] = false;
        }
       
        ProcessesState() { }
        private static readonly object lockobj = new object();
        private static ProcessesState instance = null;
        public static ProcessesState Instance
        {
            get
            {
                lock (lockobj)
                {
                    if (instance == null)
                    {
                        instance = new ProcessesState();

                    }
                    return instance;
                }
            }
        }
    }
}
