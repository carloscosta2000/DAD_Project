using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TransactionManager
{
    public class TimeStepInfo
    {

        private int current_timestep = 1;
        private static readonly object locktimestep = new object();
        private HashSet<int> crashed_time_slots = new HashSet<int>();
        private Dictionary<int, ArrayList> suspected_by_in_timeslot = new Dictionary<int, ArrayList>();
        public int get_current_timestep()
        {
            int ct = 0;
            lock (locktimestep)
                ct = this.current_timestep;
            return ct;
        }

        public void increment_timestep()
        {
            Console.WriteLine("timestep: " + current_timestep);
            lock (locktimestep)
                current_timestep++;
        }

        public void set_crashed_time_slots(HashSet<int> crashed_time_slots)
        {
            this.crashed_time_slots = crashed_time_slots;
        }
        public HashSet<int> get_crashed_time_slots()
        {
            return this.crashed_time_slots;
        }
        public void set_suspected_time_slots(Dictionary<int, ArrayList> suspected_by_in_timeslot)
        {
            this.suspected_by_in_timeslot = suspected_by_in_timeslot;
        }
        public Dictionary<int, ArrayList> get_suspected_time_slots()
        {
            return this.suspected_by_in_timeslot;
        }
        public bool isSuspect(string host)
        {
            return this.suspected_by_in_timeslot[this.current_timestep].Contains(host);
        }

        public bool isCrashed()
        {
            return this.crashed_time_slots.Contains(this.current_timestep);
        }

        TimeStepInfo() { }
        private static readonly object lockobj = new object();
        private static TimeStepInfo instance = null;
        public static TimeStepInfo Instance
        {
            get
            {
                lock (lockobj)
                {
                    if (instance == null)
                    {
                        instance = new TimeStepInfo();
                    }
                    return instance;
                }
            }
        }
    }
}