using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DADTKV
{
    public class DadInt
    {
        private string key;
        private int value;

        public string Key
        {
            get { return key; }
            set { key = value; }
        }

        public int Value
        { 
            get { return value; } 
            set { this.value = value; } 
        }

        public override string ToString()
        {
            return "DadInt<" + key + ", " + value.ToString() + ">";
        }
    }
}
