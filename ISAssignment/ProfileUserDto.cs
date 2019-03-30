using System;
using System.Collections.Generic;
using System.Linq;
using System.Numerics;
using System.Text;
using System.Threading.Tasks;


namespace ISAssignment
{
    public class ProfileUserDto
    {
        public Int32 Doctet { get; set; }
        public Int64 Rfp { get; set; }
        public Int32 Duration { get; set; }
        public Int64 MinRfpValue { get; set; }
        public Int64 MaxRfpValue { get; set; }
        public int BinValue
        {
            get
            {
                return Convert.ToInt32(Math.Floor(this.bin));
            }            
        }

        public double bin
        {
            get
            {
                return (this.bindivide == 1 ? 8063 : 8064 * this.bindivide);
            }
        }
        public double Ratio
        {
            get
            {
                return (this.Duration == 0 ? 0 : ((double)this.Doctet / (double)this.Duration)) * 1000;
            }
        }
        public double bindivide
        {
            get
            {
                return (((double)this.Rfp - (double)this.MinRfpValue) / ((double)this.MaxRfpValue - (double)this.MinRfpValue));
            }
        }
    }
}