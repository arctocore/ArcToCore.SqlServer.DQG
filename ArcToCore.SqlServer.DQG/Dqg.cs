using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using System.Text;

namespace ArcToCore.SqlServer.DQG
{
	public static class Dqg
	{
		#region Variables

		private static DataTable _dataTable;
		private static StringBuilder _sqlBuilder = new StringBuilder();
		private static int count = 0;
		private static string query = "SELECT SCHEMA_NAME(f.SCHEMA_ID) as SchemaName, OBJECT_NAME(f.parent_object_id) AS TableName, COL_NAME(fc.parent_object_id,fc.parent_column_id) AS ColumnName, SCHEMA_NAME(o.SCHEMA_ID) as ReferenceSchemaName, OBJECT_NAME (f.referenced_object_id) AS ReferenceTableName, COL_NAME(fc.referenced_object_id,fc.referenced_column_id) AS ReferenceColumnName FROM sys.foreign_keys AS f INNER JOIN sys.foreign_key_columns AS fc ON f.OBJECT_ID = fc.constraint_object_id INNER JOIN sys.objects AS o ON o.OBJECT_ID = fc.referenced_object_id";

		#endregion Variables

		#region public

		public static void GenerateStandardTableDependencyQueries(string connectionStringTxt, string dest, bool seperateFiles, bool removeFilesOnNewSqlGeneration)
		{
			if (removeFilesOnNewSqlGeneration)
			{
				try
				{
					DirectoryInfo di = new DirectoryInfo(dest);

					foreach (FileInfo file in di.GetFiles())
					{
						file.Delete();
					}
				}
				catch (Exception e)
				{
					Console.WriteLine(e);
					throw;
				}
			}
			if (string.IsNullOrEmpty(connectionStringTxt))
			{
				throw new Exception("Missing connection settings");
			}

			string connString = connectionStringTxt;

			SqlConnection conn = new SqlConnection(connString);

			var cmd = new SqlCommand(query, conn);

			conn.Open();

			// create data adapter
			var da = new System.Data.SqlClient.SqlDataAdapter(cmd);
			// this will query your database and return the result to your datatable
			_dataTable = new DataTable();
			da.Fill(_dataTable);
			conn.Close();
			da.Dispose();

			_sqlBuilder.Length = 0;
			_sqlBuilder.Capacity = 0;

			foreach (DataRow item in _dataTable.Rows)
			{
				//  0             1               2                   3             4                     5
				//SchemaName | TableName | ColumnName | ReferenceSchemaName | ReferenceTableName | ReferenceColumnName

				//if (!sqlBuilder.ToString().Contains("SELECT * FROM " + item["SchemaName"] + "." + item["TableName"] +
				//									" AS " + item["TableName"] + "_" + item["TableName"]))
				//{
				//	sqlBuilder.AppendLine("SELECT * FROM " + item["SchemaName"] + "." + item["TableName"] + " AS " +
				//						  item["TableName"] + "_" + item["TableName"]);

				//}

				if (!_sqlBuilder.ToString().Contains("SELECT * FROM [" + item["TableName"] +
													"] AS " + item["TableName"] + "_" + item["TableName"]))
				{
					_sqlBuilder.AppendLine("SELECT * FROM [" + item["TableName"] + "] AS " +
										  item["TableName"] + "_" + item["TableName"]);
				}

				string sql = SqlJoinMakerRecursive(item, item["TableName"] + "_" + item["TableName"]);
				_sqlBuilder.AppendLine();
				count = 0;

				if (seperateFiles)
				{
					string sqlOutPut = sql.Replace("{", string.Empty).Replace("}", string.Empty).ToString();
					Util.SaveOutput(item["TableName"].ToString() + "Query", dest, sqlOutPut);
					_sqlBuilder.Clear();
				}
			}

			if (!seperateFiles)
			{
				string sqlOutPut = _sqlBuilder.Replace("{", string.Empty).Replace("}", string.Empty).ToString();
				Util.SaveOutput(conn.Database, dest, sqlOutPut);
				_sqlBuilder.Clear();
			}
		}

		#endregion public

		#region private

		/// <summary>
		/// Orders the recursive helper.
		/// </summary>
		/// <param name="pdtSource">The PDT source.</param>
		/// <param name="pdtResult">The PDT result.</param>
		/// <param name="pdrCurrent">The PDR current.</param>
		/// <param name="pRecursiveLevel">The p recursive level.</param>
		/// <param name="pIDColumn">The p identifier column.</param>
		/// <param name="pPIDColumn">The p pid column.</param>
		/// <param name="pSortColumn">The p sort column.</param>

		private static string SqlJoinMakerRecursive(DataRow dr, string tableName)
		{
			List<string> sqlBuilderLst = new List<string>();

			foreach (DataRow item in dr.Table.Rows)
			{
				string innerTable = "INNER JOIN " + item["ReferenceSchemaName"] + "." + item["ReferenceTableName"];

				if (tableName.Contains(item["TableName"] + "_" + item["TableName"]))
				{
					if (_sqlBuilder.ToString().Contains(innerTable))
					{
						count++;
					}

					if (count < 2)
					{
						//if (!sqlBuilder.ToString().Contains("INNER JOIN " + item["ReferenceSchemaName"] + "." + item["ReferenceTableName"] + " AS " + item["ReferenceTableName"] + "_" + item["ReferenceTableName"]))
						//{
						//	string guid = Guid.NewGuid().ToString().Replace("-", string.Empty).Trim();
						//	sqlBuilder.AppendLine("INNER JOIN " + item["ReferenceSchemaName"] + "." + item["ReferenceTableName"] + " AS SP" + guid + "_" + item["ReferenceTableName"] + "_" + item["ReferenceTableName"]);
						//	sqlBuilder.AppendLine("ON " + item["TableName"] + "_" + item["TableName"] + "." + item["ColumnName"] + " = SP" + guid + "_" + item["ReferenceTableName"] + "_" + item["ReferenceTableName"] + "." + item["ReferenceColumnName"]);
						//}
						//else
						//{
						//	if (!sqlBuilder.ToString().Contains("INNER JOIN " + item["ReferenceSchemaName"] + "." + item["ReferenceTableName"] + " AS " + item["ReferenceTableName"] + "_" + item["ReferenceTableName"]))
						//	{
						//		string guid = Guid.NewGuid().ToString().Replace("-", string.Empty).Trim();
						//		sqlBuilder.AppendLine("INNER JOIN " + item["ReferenceSchemaName"] + "." + item["ReferenceTableName"] + " AS SP" + guid + "_" + item["ReferenceTableName"] + "_" + item["ReferenceTableName"]);
						//		sqlBuilder.AppendLine("ON " + item["TableName"] + "_" + item["TableName"] + "." + item["ColumnName"] + " = SP" + guid + "_" + item["ReferenceTableName"] + "_" + item["ReferenceTableName"] + "." + item["ReferenceColumnName"]);
						//	}
						//}

						if (!_sqlBuilder.ToString().Contains("INNER JOIN [" + item["ReferenceTableName"] + "] AS " + item["ReferenceTableName"] + "_" + item["ReferenceTableName"]))
						{
							string guid = Guid.NewGuid().ToString().Replace("-", string.Empty).Trim();
							_sqlBuilder.AppendLine("INNER JOIN [" + item["ReferenceTableName"] + "] AS SP" + guid + "_" + item["ReferenceTableName"] + "_" + item["ReferenceTableName"]);
							_sqlBuilder.AppendLine("ON " + item["TableName"] + "_" + item["TableName"] + "." + item["ColumnName"] + " = SP" + guid + "_" + item["ReferenceTableName"] + "_" + item["ReferenceTableName"] + "." + item["ReferenceColumnName"]);
						}
						else
						{
							if (!_sqlBuilder.ToString().Contains("INNER JOIN [" + item["ReferenceTableName"] + "] AS " + item["ReferenceTableName"] + "_" + item["ReferenceTableName"]))
							{
								string guid = Guid.NewGuid().ToString().Replace("-", string.Empty).Trim();
								_sqlBuilder.AppendLine("INNER JOIN [" + item["ReferenceTableName"] + "] AS SP" + guid + "_" + item["ReferenceTableName"] + "_" + item["ReferenceTableName"]);
								_sqlBuilder.AppendLine("ON " + item["TableName"] + "_" + item["TableName"] + "." + item["ColumnName"] + " = SP" + guid + "_" + item["ReferenceTableName"] + "_" + item["ReferenceTableName"] + "." + item["ReferenceColumnName"]);
							}
						}
					}
				}
			}

			return _sqlBuilder.ToString();
		}

		#endregion private
	}
}