using System;
using DbUp.Builder;
using DbUp.Engine.Transactions;
using DbUp.Oracle;
using DbUp.Oracle.Engine;

/// <summary>
/// Configuration extension methods for Oracle.
/// </summary>
/// <remarks>
/// NOTE: DO NOT MOVE THIS TO A NAMESPACE
/// Since the class just contains extension methods, we leave it in the root so that it is always discovered
/// and people don't have to manually add using statements.
/// </remarks>

// ReSharper disable CheckNamespace
public static class OracleExtensions
// ReSharper restore CheckNamespace
{
    /// <summary>
    /// Creates an upgrader for Oracle databases with oficial Oracle provider ODP.NET.
    /// </summary>
    /// <param name="supported">Fluent helper type.</param>
    /// <param name="connectionString">Database connection string</param>
    /// <returns>
    /// A builder for an Oracle database upgrader
    /// </returns>
    public static UpgradeEngineBuilder OracleDatabase(this SupportedDatabases supported, string connectionString)
    {
        return OracleDatabase(new OracleConnectionManager(connectionString));
    }

    private static UpgradeEngineBuilder OracleDatabase(IConnectionManager connectionManager)
    {
        var builder = new UpgradeEngineBuilder();
        builder.Configure(c => c.ConnectionManager = connectionManager);
        builder.Configure(c => c.ScriptExecutor = new ScriptExecutor(() => c.ConnectionManager, () => c.Log, () => c.VariablesEnabled, c.ScriptPreprocessors, "schemaversions"));
        builder.Configure(c => c.Journal = new OracleTableJournal(() => c.ConnectionManager, () => c.Log, "schemaversions"));
        return builder;
    }

    /// <summary>
    /// Tracks the list of executed scripts in a Oracle table.
    /// </summary>
    /// <param name="builder">The builder.</param>
    /// <param name="table">The name for journaling table.</param>
    /// <returns></returns>
    public static UpgradeEngineBuilder JournalToSqlTable(this UpgradeEngineBuilder builder, string table)
    {
        builder.Configure(c => c.ScriptExecutor = new ScriptExecutor(() => c.ConnectionManager, () => c.Log, () => c.VariablesEnabled, c.ScriptPreprocessors, table));
        builder.Configure(c => c.Journal = new OracleTableJournal(() => c.ConnectionManager, () => c.Log, table));
        return builder;
    }

    //TODO: Give possibility to use strict script applying or not
    //TODO: Try to use Flashback technology of Oracle
    //TODO: Throw exception when developers want's to use Transaction (because Commit on DDL)
}

