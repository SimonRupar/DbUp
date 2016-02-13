using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Data.SqlClient;
using System.Linq;
using DbUp.Engine;
using DbUp.Engine.Output;
using DbUp.Engine.Transactions;
using DbUp.Support.SqlServer;

namespace DbUp.Oracle.Engine
{
    /// <summary>
    /// An implementation of the <see cref="IJournal"/> interface which tracks version numbers for a Oracle database
    /// </summary>
    public class OracleTableJournal : SqlTableJournal
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="OracleTableJournal"/> class.
        /// </summary>
        /// <param name="connectionManager">The connection manager.</param>
        /// <param name="logger">The log.</param>
        /// <param name="table">The table name.</param>
        public OracleTableJournal(Func<IConnectionManager> connectionManager, Func<IUpgradeLog> logger, string table) : base(connectionManager, logger, string.Empty, table)
        {
        }

        protected override string GetExecutedScriptsSql(string schema, string table)
        {
            return string.Format("SELECT SCRIPT_NAME FROM {0} WHERE FAILURE_STATEMENT_INDEX IS NULL AND FAILURE_REMARK IS NULL ORDER BY SCRIPT_NAME ", table);
        }

        /// <summary>
        /// Validate already executed SqlScript with state in database.
        /// </summary>
        /// <param name="script">SqlScript to validate.</param>
        /// <returns>True if SqlScript is valid.</returns>
        public bool ValidateScript(SqlScript script)
        {
            return ValidateExecutedScript(script, null);
        }

        /// <summary>
        /// Get index of failed part in oracle script from journal database table. 
        /// </summary>
        /// <param name="script">Script to get index for failed part.</param>
        /// <returns>Index of failed part</returns>
        public int GetFailedStatementIndex(SqlScript script)
        {
            var exists = DoesTableExist();
            if (exists)
            {
                return connectionManager().ExecuteCommandsWithManagedConnection(dbCommandFactory =>
                {
                    using (var command = dbCommandFactory())
                    {
                        command.CommandText = String.Format("SELECT FAILURE_STATEMENT_INDEX FROM {0} WHERE SCRIPT_NAME = :scriptName", table);

                        var scriptNameParam = command.CreateParameter();
                        scriptNameParam.ParameterName = "scriptName";
                        scriptNameParam.Value = script.Name;
                        command.Parameters.Add(scriptNameParam);

                        command.CommandType = CommandType.Text;

                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                                return Convert.ToInt32(reader[0]);
                        }
                    }
                    return 0;
                });
            }
            return 0;
        }

        /// <summary>
        /// Get hash of sucessfully executed parts of failed oracle script from journal database table. 
        /// This is intendant for validation of already successfully executed parts of Oracle scripts, so developers don't change allready appiled changes.
        /// </summary>
        /// <param name="script">Script to get hash of sucessfully executed parts.</param>
        /// <returns>Index of failed part</returns>
        public int GetFailedStatementHash(SqlScript script)
        {
            var exists = DoesTableExist();
            if (exists)
            {
                return connectionManager().ExecuteCommandsWithManagedConnection(dbCommandFactory =>
                {
                    using (var command = dbCommandFactory())
                    {
                        command.CommandText = String.Format("SELECT SCRIPT_HASHCODE FROM {0} WHERE SCRIPT_NAME = :scriptName", table);

                        var scriptNameParam = command.CreateParameter();
                        scriptNameParam.ParameterName = "scriptName";
                        scriptNameParam.Value = script.Name;
                        command.Parameters.Add(scriptNameParam);

                        command.CommandType = CommandType.Text;

                        using (var reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                                return Convert.ToInt32(reader[0]);
                        }
                    }
                    return 0;
                });
            }
            return 0;
        }

        /// <summary>
        /// Records a database upgrade for a database specified in a given connection string.
        /// </summary>
        /// <param name="script">The script.</param>
        public new void StoreExecutedScript(SqlScript script)
        {
            StoreExecutedScript(script, null, null);
        }

        /// <summary>
        /// Records a database upgrade for a database specified in a given connection string.
        /// </summary>
        /// <param name="script">The script.</param>
        /// <param name="failureStatementIndex">Statments that were successfully executed. If null, all statements in script has been successfully executed. </param>
        /// <param name="failureRemark"/>
        public void StoreExecutedScript(SqlScript script, int? failureStatementIndex, string failureRemark)
        {
            var cManagerInstance = connectionManager();
            IEnumerable<string> successfullStatments = cManagerInstance.SplitScriptIntoCommands(script.Contents);

            if (failureStatementIndex != null)
                successfullStatments = successfullStatments.Take(Convert.ToInt32(failureStatementIndex));

            var exists = DoesTableExist();
            connectionManager().ExecuteCommandsWithManagedConnection(dbCommandFactory =>
            {
                if (!exists)
                {
                    log().WriteInformation(string.Format("Creating the {0} table", table));

                    using (var command = dbCommandFactory())
                    {
                        command.CommandText = String.Format(@"CREATE TABLE {0} (ID integer GENERATED ALWAYS AS IDENTITY(start with 1 increment by 1 nocycle),
                      SCRIPT_NAME VARCHAR2(255) NOT NULL,
                      APPLIED TIMESTAMP NOT NULL,
                      REMARK VARCHAR2(4000) NULL,
                      FAILURE_STATEMENT_INDEX integer NULL,
                      FAILURE_REMARK VARCHAR2(4000) NULL,
                      SCRIPT_HASHCODE integer NULL,
                      CONSTRAINT PK_{0} PRIMARY KEY (ID) ENABLE VALIDATE)", table);

                        command.CommandType = CommandType.Text;
                        command.ExecuteNonQuery();
                    }

                    log().WriteInformation(string.Format("The {0} table has been created", table));
                }
                else
                {
                    using (var command = dbCommandFactory())
                    {
                        command.CommandText = String.Format(@"DELETE FROM {0} WHERE SCRIPT_NAME = :scriptName AND FAILURE_STATEMENT_INDEX IS NOT NULL AND FAILURE_REMARK IS NOT NULL", table);

                        var scriptNameParam = command.CreateParameter();
                        scriptNameParam.ParameterName = "scriptName";
                        scriptNameParam.Value = script.Name;
                        command.Parameters.Add(scriptNameParam);

                        command.CommandType = CommandType.Text;
                        command.ExecuteNonQuery();
                    }
                }


                using (var command = dbCommandFactory())
                {
                    command.CommandText = String.Format("INSERT INTO {0} (SCRIPT_NAME, APPLIED, FAILURE_STATEMENT_INDEX, FAILURE_REMARK, SCRIPT_HASHCODE ) " +
                                 "VALUES (:scriptName, TO_DATE(:applied, 'yyyy-mm-dd hh24:mi:ss'), :failureStatementIndex, :failureRemark, :hash)", table);

                    var scriptNameParam = command.CreateParameter();
                    scriptNameParam.ParameterName = "scriptName";
                    scriptNameParam.Value = script.Name;
                    command.Parameters.Add(scriptNameParam);

                    var appliedParam = command.CreateParameter();
                    appliedParam.ParameterName = "applied";
                    appliedParam.Value = String.Format("{0:yyyy-MM-dd hh:mm:ss}", DateTime.UtcNow);
                    command.Parameters.Add(appliedParam);

                    var successfullStatementIndexParam = command.CreateParameter();
                    successfullStatementIndexParam.ParameterName = "failureStatementIndex";
                    successfullStatementIndexParam.Value = failureStatementIndex;
                    command.Parameters.Add(successfullStatementIndexParam);

                    var failureRemarkParam = command.CreateParameter();
                    failureRemarkParam.ParameterName = "failureRemark";
                    failureRemarkParam.Value = failureRemark;
                    command.Parameters.Add(failureRemarkParam);

                    var hashParam = command.CreateParameter();
                    hashParam.ParameterName = "hash";
                    hashParam.Value = CalculateHash(successfullStatments);
                    command.Parameters.Add(hashParam);

                    command.CommandType = CommandType.Text;
                    command.ExecuteNonQuery();
                }
            });
        }

        /// <summary>
        /// Check if already executed part of script has changed.
        /// </summary>
        /// <param name="script">The script.</param>
        /// <param name="successfullyExecutedStatements">Collection of already successfull executed statements of scipt</param>
        /// <returns>Return true if already executed statements of scripts have not changed.</returns>
        public bool ValidateExecutedScript(SqlScript script, IEnumerable<string> successfullyExecutedStatements)
        {
            if (successfullyExecutedStatements == null)
            {
                var cManagerInstance = connectionManager();
                successfullyExecutedStatements = cManagerInstance.SplitScriptIntoCommands(script.Contents);
            }

            int successfullHash = GetFailedStatementHash(script);
            int scriptsHash = CalculateHash(successfullyExecutedStatements);
            return successfullHash == scriptsHash;
        }

        protected override bool VerifyTableExistsCommand(IDbCommand command, string tableName, string schemaName)
        {
            command.CommandText = string.Format("select table_name from user_tables where table_name=upper('{0}'", tableName);
            command.CommandType = CommandType.Text;
            var result = command.ExecuteScalar() as int?;
            return result == 1;
        }

        private static int CalculateHash(IEnumerable<string> collection)
        {
            if (collection == null || !collection.Any()) return 0;
            return collection.Aggregate(0, (current, entry) => current ^ entry.GetHashCode());
        }
    }
}
