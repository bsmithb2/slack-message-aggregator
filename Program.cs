using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using SlackAPI;

namespace slack_app
{
    class Program
    {
        static object key = new object();
        static int MAX_DEGREE_OF_PARALLELISM = 1;	
        static SemaphoreSlim _semaphore = new SemaphoreSlim(MAX_DEGREE_OF_PARALLELISM);	
        static int threadCount = 0;
        static List<String> peoples = new List<String>();
        static async Task Main(string[] args)
        {
            string TOKEN = Environment.GetEnvironmentVariable("SLACK_TOKEN");  
            if(String.IsNullOrEmpty(TOKEN)) {
                throw new Exception("Please provide a SLACK_TOKEN environment variable");
            }
            var dop = Environment.GetEnvironmentVariable("SLACK_MAXDOP");
            var dopValue = 1;
            if(dop != null && Int32.TryParse(dop, out dopValue)) {
                MAX_DEGREE_OF_PARALLELISM = dopValue;
            }
            Console.WriteLine("Parallel processing threads: " + MAX_DEGREE_OF_PARALLELISM);
            var slackClient = new SlackTaskClient(TOKEN);

            var list = await slackClient.GetChannelListAsync();
            Console.WriteLine("channels: " + list.channels.Count());
            var users = await slackClient.GetUserListAsync();
            Console.WriteLine("users: " + users.members.Count());
            
            var tasks = new List<Task>();
            
            foreach(var channel in list.channels) {
                tasks.Add(CheckStuff(slackClient, channel, users));
            }
            await Task.WhenAll(tasks.ToArray());
            
            Console.WriteLine("");
            Console.WriteLine("------------------------------------------------------------");
            Console.WriteLine("");
            var counts = peoples.GroupBy(s => s).OrderByDescending(x => x.Count());
            foreach(var cnt in counts) {
                Console.WriteLine(cnt.Key + ": " + cnt.Count());
            }

        }

        static async Task CheckStuff(SlackTaskClient slackClient, Channel channel, UserListResponse users) {
            await _semaphore.WaitAsync();
            threadCount++;
            //Console.WriteLine("Thread {0} starting", threadCount);
            try {
                var history = await slackClient.GetChannelHistoryAsync(channel, null, DateTime.Now.AddDays(-7), 1000);
                if(history.messages == null) {
                    Console.WriteLine("Rate Limited: Backing Off for 10s");
                    await Task.Delay(10000);
                    history = await slackClient.GetChannelHistoryAsync(channel, null, DateTime.Now.AddDays(-7), 1000);

                    if(history.messages == null) {
                        throw new Exception("Something went bad. You was rate limited");
                    }
                }
                if(history.messages.Count() == 0) {
                    return;
                }

                Console.WriteLine("{0}: Count: {1}, More Messages? : {2}", channel.name, history.messages.Count(), history.has_more);
                foreach(var message in history.messages) {
                    var user = users.members.FirstOrDefault(s => s.id == message.user);
                    var name = user != null ? user.name : "bot or unknown";
                    lock(key) {
                        peoples.Add(name);
                    }
                }
            }
            catch(Exception e) {
                Console.WriteLine("Exception: " + e.Message);
                throw;
            }
            finally 
            {
                //Console.WriteLine("Thread {0} finished", threadCount);
                threadCount--;
                _semaphore.Release();
            }
        }
    }
}
