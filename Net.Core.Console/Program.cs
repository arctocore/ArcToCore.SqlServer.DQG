using ArcToCore.SqlServer.DQG;

namespace Net.Core.Console
{
	internal class Program
	{
		private static void Main(string[] args)
		{
			Dqg.GenerateStandardTableDependencyQueries(
				@"Server=XXX; Database=XXX; User ID=XXX; Password=XXX;",
				@"c:\tmp\sqlcode\",
				true,
				true);
		}
	}
}