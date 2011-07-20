using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using MySql.Data.MySqlClient;
using Dapper;
using System.Diagnostics;
using System.Data;

namespace ForeignKeyPerformanceTest
{
    class Program
    {
        public static IDbConnection OpenConnetion()
        {
            var connection = new MySqlConnection("Server=localhost;UserId=root;Password=123456;Database=foreignkeytest");
            connection.Open();

            return connection;
        }

        static void Main(string[] args)
        {

            var tester = new ForeignKeyPerformanceTester();

            using (var connection = OpenConnetion())
            {
                //清除数据
                connection.Execute(@"
                    TRUNCATE `nfkm`;
                    TRUNCATE `hfkm`;");

            }

            //准备测试数据
            int slaveDataCount = 1000000;
            int masterTestCount = 3000;
            int warmupDataCount = 100;

            Random random = new Random();

            //预热
            var warmupDatas = Enumerable.Range(1, warmupDataCount).
               Select(i => new
               {
                   Id = i,
                   SlaveId = random.Next(1, slaveDataCount),
                   Text = Guid.NewGuid().ToString()
               }).ToList();

            tester.Test(warmupDatas, (c, t, data) =>
            {
                c.Execute(@"insert nfkm(Id, SlaveId, Text) values (@Id, @SlaveId, @Text)", data, t);
                c.Execute(@"insert hfkm(Id, SlaveId, Text) values (@Id, @SlaveId, @Text)", data, t);
                c.Execute(@"delete from nfkm where Id=@Id", data, t);
                c.Execute(@"delete from hfkm where Id=@Id", data, t);
            });

            //插入测试
            var insertDatas = Enumerable.Range(1, masterTestCount).
                Select(i => new
                {
                    Id = i,
                    SlaveId = random.Next(1, slaveDataCount),
                    Text = Guid.NewGuid().ToString()
                }).ToList();

            tester.Test(insertDatas, (c, t, data) =>
            {
                c.Execute(@"insert nfkm(Id, SlaveId, Text) values (@Id, @SlaveId, @Text)", data, t);
            }, time =>
            {
                Console.WriteLine("无外键主表，随机关联有{0}条数据的从表，测试插入{1}条数据，耗时：{2}ms。", slaveDataCount, masterTestCount, time);
            });

            tester.Test(insertDatas, (c, t, data) =>
            {
                c.Execute(@"insert hfkm(Id, SlaveId, Text) values (@Id, @SlaveId, @Text)", data, t);
            }, time =>
            {
                Console.WriteLine("有外键主表，随机关联有{0}条数据的从表，测试插入{1}条数据，耗时：{2}ms。", slaveDataCount, masterTestCount, time);
            });

            //更新测试
            var updateDatas = Enumerable.Range(1, masterTestCount).
                Select(i => new
                {
                    Id = i,
                    SlaveId = random.Next(1, slaveDataCount),
                    Text = Guid.NewGuid().ToString()
                }).ToList();

            tester.Test(updateDatas, (c, t, data) =>
            {
                c.Execute(@"update nfkm set Text=@Text, SlaveId=@SlaveId where Id=@Id", data, t);
            }, time =>
            {
                Console.WriteLine("无外键主表，随机关联有{0}条数据的从表，测试更新{1}条数据，耗时：{2}ms。", slaveDataCount, masterTestCount, time);
            });

            tester.Test(updateDatas, (c, t, data) =>
            {
                c.Execute(@"update hfkm set Text=@Text, SlaveId=@SlaveId where Id=@Id", data, t);
            }, time =>
            {
                Console.WriteLine("有外键主表，随机关联有{0}条数据的从表，测试更新{1}条数据，耗时：{2}ms。", slaveDataCount, masterTestCount, time);
            });

            //删除测试
            var deleteDatas = Enumerable.Range(1, masterTestCount).
                Select(i => new
                {
                    Id = i
                }).ToList();

            tester.Test(deleteDatas, (c, t, data) =>
            {
                c.Execute(@"delete from nfkm where Id=@Id", data, t);
            }, time =>
            {
                Console.WriteLine("无外键主表，随机关联有{0}条数据的从表，测试删除{1}条数据，耗时：{2}ms。", slaveDataCount, masterTestCount, time);
            });

            tester.Test(deleteDatas, (c, t, data) =>
            {
                c.Execute(@"delete from hfkm where Id=@Id", data, t);
            }, time =>
            {
                Console.WriteLine("有外键主表，随机关联有{0}条数据的从表，测试删除{1}条数据，耗时：{2}ms。", slaveDataCount, masterTestCount, time);
            });

        }
    }

    public class ForeignKeyPerformanceTester
    {

        public ForeignKeyPerformanceTester()
        {
        }

        public void Test(IEnumerable<Object> datas, Action<IDbConnection, IDbTransaction, Object> testAction, Action<long> afterTestAction = null)
        {
            GC.Collect();

            var watch = Stopwatch.StartNew();

            foreach (var data in datas)
            {
                using (var connection = Program.OpenConnetion())
                using (var transaction = connection.BeginTransaction())
                {
                    testAction(connection, transaction, data);

                    transaction.Commit();
                }
            }

            ////暂时设为单任务
            //datas.AsParallel().WithDegreeOfParallelism(1).ForAll(data =>
            //{
            //    var connection = OpenConnetion();
            //    var transaction = connection.BeginTransaction();

            //    testAction(connection, transaction, data);

            //    transaction.Commit();
            //    transaction.Dispose();
            //    connection.Close();
            //});

            watch.Stop();

            if (afterTestAction != null)
                afterTestAction(watch.ElapsedMilliseconds);
        }

    }
}
