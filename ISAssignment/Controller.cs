using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using ExcelDataReader;


namespace ISAssignment
{
    /// <summary>
    /// This the class, which called in Main Method
    /// </summary>
    public class Controller
    {
        #region " Member Variables  ... "


        #endregion

        /// <summary>
        /// Constructor
        /// </summary>
        public Controller()
        {

        }

        #region " Public Procedures ... "

        /// <summary>
        /// This is the main method, for executing program.
        /// It takes input directory from Console and read each file in parallel
        /// </summary>
        public void Run()
        {
            Console.WriteLine("Please Enter the complete directory path");
            Console.WriteLine(@"For example: C:\Excel\ --> Which has all xlsx files.");

            string file1 = Console.ReadLine();

            var files = Directory.GetFiles(@file1);

            ParallelOptions po = new ParallelOptions { MaxDegreeOfParallelism = 12 };

            foreach (var file in files)
            {
                Parallel.ForEach(files, po, i =>
                 {
                     Process(file, i);
                 });
            }

            Console.ReadLine();
            Console.WriteLine("Please Press Enter to close the Application");
        }

        /// <summary>
        /// This Process takes input as two excel files and Reads complete excel file and keeps in Cache
        /// The data is passed to next method and goes on with other methods applying business rules
        /// </summary>
        /// <param name="file1"></param>
        /// <param name="file2"></param>
        public void Process(string file1, string file2)
        {
            try
            {
                var user1 = ConvertExcelToDataTable1(file1);
                var user2 = ConvertExcelToDataTable2(file2);

                Console.WriteLine("File 1 --->" + file1 + "   File    --->" + file2);
                Console.WriteLine("-------------------------------------------------");

                CaculateOutput(user1, user2);

                Console.WriteLine("-------------------------------------------------");
            }

            catch (Exception ex)
            {
                Console.WriteLine(ex);
            }
        }

        #endregion        

        #region " Private Procedures ... "

        /// <summary>
        /// This Method removes data which has 0 ratio and data is passed to next method
        /// </summary>
        private void CaculateOutput(List<ProfileUserDto> user1, List<ProfileUserDto> user2)
        {
            ParallelOptions po = new ParallelOptions { MaxDegreeOfParallelism = 2 };

            Parallel.Invoke(po,
               () => user1.RemoveAll(i => i.Ratio == 0),
               () => user2.RemoveAll(i => i.Ratio == 0));

            OneRatio(user1, user2);
        }

        /// <summary>
        /// This Method calculates ratios, which has one bin value for a given data and data is passed to next method
        /// </summary>
        private void OneRatio(List<ProfileUserDto> user1, List<ProfileUserDto> user2)
        {
            ParallelOptions po = new ParallelOptions { MaxDegreeOfParallelism = 2 };

            List<int> user1Bin = new List<int>();
            List<int> user2Bin = new List<int>();

            List<RatioDto> user1Ratio = new List<RatioDto>();
            List<RatioDto> user2Ratio = new List<RatioDto>();

            var user1oneratio = (from u in user1
                                 group u by u.BinValue into grp
                                 where grp.Count() == 1
                                 select grp.First()).ToList();
            var user2oneratio = (from u in user2
                                 group u by u.BinValue into grp
                                 where grp.Count() == 1
                                 select grp.First()).ToList();

            user1Bin.AddRange(user1oneratio.Select(i => i.BinValue).ToList());
            user2Bin.AddRange(user2oneratio.Select(i => i.BinValue).ToList());

            Parallel.Invoke(po,
               () => user1oneratio.ForEach(a => user1Ratio.Add(new RatioDto { BinValue = a.BinValue, Ratio = a.Ratio })),
           () => user2oneratio.ForEach(a => user2Ratio.Add(new RatioDto { BinValue = a.BinValue, Ratio = a.Ratio })));

            Parallel.Invoke(po,
               () => user1.RemoveAll(i => user1Bin.Contains(i.BinValue)),
               () => user2.RemoveAll(i => user2Bin.Contains(i.BinValue)));

            MoreThanOneRatio(user1, user2, user1Ratio, user2Ratio);
        }

        /// <summary>
        /// This Method calculates ratios, which has more than one bin value for a given data and data is passed to next method
        /// </summary>
        private void MoreThanOneRatio(List<ProfileUserDto> user1, List<ProfileUserDto> user2, List<RatioDto> user1Ratio, List<RatioDto> user2Ratio)
        {
            ParallelOptions po = new ParallelOptions { MaxDegreeOfParallelism = 3 };

            List<int> user1Bin = new List<int>();
            List<int> user2Bin = new List<int>();

            var user1moreratio = user1.GroupBy(i => i.BinValue).Where(grp => grp.Count() > 1).ToList();
            var user2moreratio = user2.GroupBy(i => i.BinValue).Where(grp => grp.Count() > 1).ToList();

            user1Bin.AddRange(user1moreratio.Select(i => i.Key).ToList());
            user2Bin.AddRange(user2moreratio.Select(i => i.Key).ToList());

            var newuser1ratio = MoreUser1(user1moreratio, user1Ratio);
            var newuser2ratio = MoreUser2(user2moreratio, user2Ratio);

            Parallel.Invoke(po,
             () => user1.RemoveAll(i => user1Bin.Contains(i.BinValue)),
             () => user2.RemoveAll(i => user2Bin.Contains(i.BinValue)));

            CalculateRatios(newuser1ratio, newuser2ratio);
        }

        /// <summary>
        /// 
        /// </summary>
        private List<RatioDto> MoreUser1(List<IGrouping<int, ProfileUserDto>> users1, List<RatioDto> user1Ratio)
        {
            foreach (var grp in users1)
            {
                var ratio = CalCulateSingleRatio(grp.ToList());
                user1Ratio.Add(new RatioDto { BinValue = grp.Key, Ratio = ratio });
            }

            return user1Ratio;
        }

        /// <summary>
        /// 
        /// </summary>
        private List<RatioDto> MoreUser2(List<IGrouping<int, ProfileUserDto>> users2, List<RatioDto> user2Ratio)
        {
            foreach (var grp in users2)
            {
                var ratio = CalCulateSingleRatio(grp.ToList());
                user2Ratio.Add(new RatioDto { BinValue = grp.Key, Ratio = ratio });
            }

            return user2Ratio;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <returns></returns>
        private double CalCulateSingleRatio(List<ProfileUserDto> profiles)
        {
            return (profiles.Select(i => i.Ratio).ToList().Sum() / profiles.Count);
        }

        /// <summary>
        /// This Method caculates ratios for a 5 minute interval  (8064)
        /// </summary>
        private void CalculateRatios(List<RatioDto> user1Ratio, List<RatioDto> user2Ratio)
        {
            for (int i = 0; i <= 8063; i++)
            {
                if (user1Ratio.Where(a => a.BinValue == i).ToList().Count == 0)
                    user1Ratio.Add(new RatioDto { BinValue = i, Ratio = 0 });

                if (user2Ratio.Where(a => a.BinValue == i).ToList().Count == 0)
                    user2Ratio.Add(new RatioDto { BinValue = i, Ratio = 0 });
            }

            user1Ratio = user1Ratio.OrderBy(i => i.BinValue).ToList();
            user2Ratio = user2Ratio.OrderBy(i => i.BinValue).ToList();

            CalculateWeeklyAndIndexesRatios(user1Ratio, user2Ratio);
        }

        /// <summary>
        /// This Method caculates Z, P and 1-P values
        /// </summary>
        private void CalculateWeeklyAndIndexesRatios(List<RatioDto> user1ratio, List<RatioDto> user2ratio)
        {
            var _week1A = user1ratio.Where(i => i.BinValue <= 2016).Select(i => i.Ratio).ToList();
            var _week2A = user1ratio.Where(i => i.BinValue > 2016 && i.BinValue <= 4032).Select(i => i.Ratio).ToList();

            var _week1B = user2ratio.Where(i => i.BinValue <= 2016).Select(i => i.Ratio).ToList();
            var _week2B = user2ratio.Where(i => i.BinValue > 2016 && i.BinValue <= 4032).Select(i => i.Ratio).ToList();

            List<double> indexWeek1A = new List<double>();
            List<double> indexWeek2A = new List<double>();
            List<double> indexWeek1B = new List<double>();
            List<double> indexWeek2B = new List<double>();

            var sortedWeek1A = _week1A.OrderBy(i => i).ToList();
            var sortedWeek2A = _week2A.OrderBy(i => i).ToList();
            var sortedWeek1B = _week1B.OrderBy(i => i).ToList();
            var sortedWeek2B = _week2B.OrderBy(i => i).ToList();

            foreach (var i in _week1A)
            {
                int index = sortedWeek1A.IndexOf(i);
                indexWeek1A.Add(index);
            }

            foreach (var i in _week2A)
            {
                int index = sortedWeek2A.IndexOf(i);
                indexWeek2A.Add(index);
            }

            foreach (var i in _week1B)
            {
                int index = sortedWeek1B.IndexOf(i);
                indexWeek1B.Add(index);
            }

            foreach (var i in _week2B)
            {
                int index = sortedWeek2B.IndexOf(i);
                indexWeek2B.Add(index);
            }

            _week1A.Clear();
            _week2A.Clear();
            _week1B.Clear();
            _week2B.Clear();

            _week1A.AddRange(indexWeek1A);
            _week2A.AddRange(indexWeek2A);
            _week1B.AddRange(indexWeek1B);
            _week2B.AddRange(indexWeek2B);

            var Week1A2A = CalculateMeanSd(_week1A, _week2A);
            var Week1A2B = CalculateMeanSd(_week1A, _week2B);
            var Week2A2B = CalculateMeanSd(_week2A, _week2B);

            var Z = CalculateZ(Week1A2A, Week1A2B, Week2A2B);
            var P = PFunction(Z);
            // Final Output
            var final = 1 - P;

            Console.WriteLine("r1A2A -> " + Week1A2A);
            Console.WriteLine("r1A2B -> " + Week1A2B);
            Console.WriteLine("r2A2B -> " + Week2A2B);

            Console.WriteLine("Z -> " + Z);
            Console.WriteLine("P -> " + P);


            Console.WriteLine("1-P -> " + final);
        }

        /// <summary>
        /// 
        /// </summary>
        private double CalculateMeanSd(List<double> XList, List<double> YList)
        {
            var meanX = XList.Average();
            var meanY = YList.Average();

            double tempSumX = 0;
            double tempSumY = 0;
            double proSum = 0;

            for (int i = 0; i < XList.Count; i++)
            {
                var tempX = XList.IndexOf(i) - meanX;
                var tempY = YList.IndexOf(i) - meanY;

                tempSumX = tempSumX + Math.Pow(tempX, 2);
                tempSumY = tempSumY + Math.Pow(tempY, 2);
                proSum = proSum + (tempX * tempY);
            }

            var stdevX = Math.Sqrt(tempSumX / XList.Count);
            var stdevY = Math.Sqrt(tempSumY / XList.Count);

            var covXY = proSum / XList.Count;

            return covXY / (stdevX * stdevY);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="r1a2a"></param>
        /// <param name="r1a2b"></param>
        /// <param name="r2a2b"></param>
        private double CalculateZ(double r1a2a, double r1a2b, double r2a2b)
        {
            if (r1a2a == 1)
                r1a2a = 0.9999;
            if (r2a2b == 1)
                r2a2b = 0.9999;

            var Z1a2a = (0.5) * Math.Log((1 + r1a2a) / (1 - r1a2a));
            var Z1a2b = (0.5) * Math.Log((1 + r1a2b) / (1 - r1a2b));

            var rm = (Math.Pow(r1a2a, 2) + Math.Pow(r1a2b, 2)) / 2;
            var f = (1 - r2a2b) / (2 * (1 - rm));
            var h = (1 - (f * rm)) / (1 - rm);

            var Z1 = (Z1a2a - Z1a2b);
            var Z2 = Math.Sqrt(2016 - 3) / (2 * (1 - r2a2b) * (h));

            return Z1 * Z2;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="z"></param>
        /// <returns></returns>
        public double PFunction(double z)
        {
            double p = 0.3275911;
            double a1 = 0.254829592;
            double a2 = -0.284496736;
            double a3 = 1.421413741;
            double a4 = -1.453152027;
            double a5 = 1.061405429;
            int sign;
            if (z < 0.0)
                sign = -1;
            else
                sign = 1;
            double x = Math.Abs(z) / Math.Sqrt(2.0);
            double t = 1.0 / (1.0 + p * x);
            double erf = 1.0 - (((((a5 * t + a4) * t) + a3)
            * t + a2) * t + a1) * t * Math.Exp(-x * x);
            return 0.5 * (1.0 + sign * erf);
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="filePath"></param>
        /// <returns></returns>
        public List<ProfileUserDto> ConvertExcelToDataTable1(string filePath)
        {
            FileStream stream = null;
            IExcelDataReader excelReader = null;
            DataTable dataTable = null;
            stream = File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);

            using (stream)
            {
                excelReader = ExcelReaderFactory.CreateOpenXmlReader(stream);
                var result = excelReader.AsDataSet(new ExcelDataSetConfiguration()
                {
                    ConfigureDataTable = (_) => new ExcelDataTableConfiguration()
                    {
                        UseHeaderRow = true
                    }
                });
                if (result != null && result.Tables.Count > 0)
                    dataTable = result.Tables[0];
            }

            var minVal = 1359698372176;
            var maxVal = 1362117596441;

            return (from DataRow dr in dataTable.Rows
                    select new ProfileUserDto
                    {
                        Doctet = Convert.ToInt32(dr["doctets"]),
                        Rfp = Convert.ToInt64(dr["Real First Packet"]),
                        Duration = Convert.ToInt32(dr["Duration"]),
                        MinRfpValue = minVal,
                        MaxRfpValue = maxVal
                    }
                       ).ToList();
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="filePath"></param>
        /// <returns></returns>
        public List<ProfileUserDto> ConvertExcelToDataTable2(string filePath)
        {
            FileStream stream = null;
            IExcelDataReader excelReader = null;
            DataTable dataTable = null;
            stream = File.Open(filePath, FileMode.Open, FileAccess.Read, FileShare.Read);

            using (stream)
            {
                excelReader = ExcelReaderFactory.CreateOpenXmlReader(stream);
                var result = excelReader.AsDataSet(new ExcelDataSetConfiguration()
                {
                    ConfigureDataTable = (_) => new ExcelDataTableConfiguration()
                    {
                        UseHeaderRow = true
                    }
                });
                if (result != null && result.Tables.Count > 0)
                    dataTable = result.Tables[0];
            }

            var minVal = 1359698372176;
            var maxVal = 1362117596441;

            return (from DataRow dr in dataTable.Rows
                    select new ProfileUserDto
                    {
                        Doctet = Convert.ToInt32(dr["doctets"]),
                        Rfp = Convert.ToInt64(dr["Real First Packet"]),
                        Duration = Convert.ToInt32(dr["Duration"]),
                        MinRfpValue = minVal,
                        MaxRfpValue = maxVal
                    }
                       ).ToList();
        }

        #endregion
    }
}