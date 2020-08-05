using System.IO;
using System.Text;

namespace ArcToCore.SqlServer.DQG
{
	public class Util
	{
		public static string PutIntoQuotes(string value)
		{
			return "\"" + value + "\"";
		}

		public static void SaveOutput(string outputFileName, string destinationFolder, string code)
		{
			using (FileStream fs = File.Create(destinationFolder + outputFileName + ".sql"))
			{
				byte[] codeByte = Encoding.UTF8.GetBytes(code);
				fs.Write(codeByte, 0, codeByte.Length);
			}
		}
	}
}