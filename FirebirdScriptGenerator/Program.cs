using System;
using System.IO;
using System.Text;
using System.Linq;
using System.Collections.Generic;
using FirebirdSql.Data.FirebirdClient;

namespace DbMetaTool
{
    public static class Program
    {
        // Przykładowe wywołania:
        // DbMetaTool build-db --db-dir "C:\db\fb5" --scripts-dir "C:\scripts"
        // DbMetaTool export-scripts --connection-string "..." --output-dir "C:\out"
        // DbMetaTool update-db --connection-string "..." --scripts-dir "C:\scripts"
        public static int Main(string[] args)
        {
            if (args.Length == 0)
            {
                Console.WriteLine("Użycie:");
                Console.WriteLine("  build-db --db-dir <ścieżka> --scripts-dir <ścieżka>");
                Console.WriteLine("  export-scripts --connection-string <connStr> --output-dir <ścieżka>");
                Console.WriteLine("  update-db --connection-string <connStr> --scripts-dir <ścieżka>");
                return 1;
            }

            try
            {
                var command = args[0].ToLowerInvariant();

                switch (command)
                {
                    case "build-db":
                        {
                            string dbDir = GetArgValue(args, "--db-dir");
                            string scriptsDir = GetArgValue(args, "--scripts-dir");

                            BuildDatabase(dbDir, scriptsDir);
                            Console.WriteLine("Baza danych została zbudowana pomyślnie.");
                            return 0;
                        }

                    case "export-scripts":
                        {
                            string connStr = GetArgValue(args, "--connection-string");
                            string outputDir = GetArgValue(args, "--output-dir");

                            ExportScripts(connStr, outputDir);
                            Console.WriteLine("Skrypty zostały wyeksportowane pomyślnie.");
                            return 0;
                        }

                    case "update-db":
                        {
                            string connStr = GetArgValue(args, "--connection-string");
                            string scriptsDir = GetArgValue(args, "--scripts-dir");

                            UpdateDatabase(connStr, scriptsDir);
                            Console.WriteLine("Baza danych została zaktualizowana pomyślnie.");
                            return 0;
                        }

                    default:
                        Console.WriteLine($"Nieznane polecenie: {command}");
                        return 1;
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Błąd: " + ex.Message);
                return -1;
            }
        }

        private static string GetArgValue(string[] args, string name)
        {
            int idx = Array.IndexOf(args, name);
            if (idx == -1 || idx + 1 >= args.Length)
                throw new ArgumentException($"Brak wymaganego parametru {name}");
            return args[idx + 1];
        }

        /// <summary>
        /// Buduje nową bazę danych Firebird 5.0 na podstawie skryptów.
        /// </summary>
        public static void BuildDatabase(string databaseDirectory, string scriptsDirectory)
        {
            // 1) Utwórz pustą bazę danych FB 5.0 w katalogu databaseDirectory.
            Directory.CreateDirectory(databaseDirectory);
            string dbPath = Path.Combine(databaseDirectory, "database.fdb");

            if (File.Exists(dbPath))
                throw new InvalidOperationException($"Plik bazy już istnieje: {dbPath} (usuń go albo użyj innego katalogu).");

            string connectionString =
                $"User ID=SYSDBA;Password=masterkey;" +
                $"Database={dbPath};DataSource=127.0.0.1;Port=3050;ServerType=0;Charset=UTF8;";

            FbConnection.CreateDatabase(connectionString, 4096, true);

            // 2) Wczytaj i wykonaj kolejno skrypty z katalogu scriptsDirectory
            //    (tylko domeny, tabele, procedury).
            using var connection = new FbConnection(connectionString);
            connection.Open();

            // 3) Obsłuż błędy i wyświetl raport.
            ExecuteSqlFiles(connection, scriptsDirectory);
        }

        /// <summary>
        /// Generuje skrypty metadanych z istniejącej bazy danych Firebird 5.0.
        /// </summary>
        public static void ExportScripts(string connectionString, string outputDirectory)
        {
            // 1) Połącz się z bazą danych przy użyciu connectionString.
            Directory.CreateDirectory(outputDirectory);
            using var connection = new FbConnection(connectionString);
            connection.Open();

            // 2) Pobierz metadane domen, tabel (z kolumnami) i procedur.
            // 3) Wygeneruj pliki .sql w outputDirectory.
            ExportDomains(connection, outputDirectory);
            ExportTables(connection, outputDirectory);
            ExportProcedures(connection, outputDirectory);
        }

        /// <summary>
        /// Aktualizuje istniejącą bazę danych Firebird 5.0 na podstawie skryptów.
        /// </summary>
        public static void UpdateDatabase(string connectionString, string scriptsDirectory)
        {
            // 1) Połącz się z bazą danych przy użyciu connectionString.
            using var connection = new FbConnection(connectionString);
            connection.Open();

            // 2) Wykonaj skrypty z katalogu scriptsDirectory (tylko obsługiwane elementy).
            // 3) Zadbaj o poprawną kolejność i bezpieczeństwo zmian.
            ExecuteSqlFiles(connection, scriptsDirectory);
        }

        // ---------------------------
        // Script runner (SET TERM + multiple statements)
        // ---------------------------

        private static void ExecuteSqlFiles(FbConnection connection, string scriptsDirectory)
        {
            if (!Directory.Exists(scriptsDirectory))
                throw new DirectoryNotFoundException($"Nie znaleziono katalogu skryptów: {scriptsDirectory}");

            var files = Directory.GetFiles(scriptsDirectory, "*.sql")
                                 .OrderBy(f => f, StringComparer.OrdinalIgnoreCase)
                                 .ToArray();

            if (files.Length == 0)
            {
                Console.WriteLine($"Brak plików .sql w {scriptsDirectory}");
                return;
            }

            foreach (var file in files)
            {
                string raw = File.ReadAllText(file);
                if (string.IsNullOrWhiteSpace(raw))
                {
                    Console.WriteLine($"Pominięto pusty plik: {Path.GetFileName(file)}");
                    continue;
                }

                try
                {
                    RunScript(connection, raw);
                    Console.WriteLine($"Wykonano: {Path.GetFileName(file)}");
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"Błąd w pliku {file}: {ex.Message}");
                }
            }
        }

        // Very small script parser:
        // - supports SET TERM <X> ; to change terminator
        // - splits by current terminator
        private static void RunScript(FbConnection connection, string script)
        {
            string terminator = ";";
            var sb = new StringBuilder();

            using var tx = connection.BeginTransaction();
            try
            {
                using var sr = new StringReader(script);
                string? line;
                while ((line = sr.ReadLine()) != null)
                {
                    var trimmed = line.Trim();

                    // Ignore empty lines and pure comment lines
                    if (trimmed.Length == 0 || trimmed.StartsWith("--"))
                    {
                        sb.AppendLine(line);
                        continue;
                    }

                    // SET TERM handling (isql-like)
                    if (trimmed.StartsWith("SET TERM", StringComparison.OrdinalIgnoreCase))
                    {
                        // Example: SET TERM ^ ;
                        // We treat everything already collected as complete (should be none)
                        var parts = trimmed.Split(new[] { ' ', '\t' }, StringSplitOptions.RemoveEmptyEntries);
                        if (parts.Length >= 3)
                        {
                            terminator = parts[2]; // e.g. "^"
                            // do not execute SET TERM itself
                            continue;
                        }
                    }

                    sb.AppendLine(line);

                    // If buffer ends with current terminator, execute
                    if (EndsWithTerminator(sb, terminator))
                    {
                        string cmdText = RemoveLastTerminator(sb.ToString(), terminator).Trim();
                        sb.Clear();

                        if (string.IsNullOrWhiteSpace(cmdText))
                            continue;

                        using var cmd = new FbCommand(cmdText, connection, tx);
                        cmd.ExecuteNonQuery();
                    }
                }

                // leftover (no terminator at end)
                string tail = sb.ToString().Trim();
                if (!string.IsNullOrWhiteSpace(tail))
                {
                    using var cmd = new FbCommand(tail, connection, tx);
                    cmd.ExecuteNonQuery();
                }

                tx.Commit();
            }
            catch
            {
                tx.Rollback();
                throw;
            }
        }

        private static bool EndsWithTerminator(StringBuilder sb, string terminator)
        {
            if (sb.Length < terminator.Length) return false;

            // trim trailing whitespace
            int i = sb.Length - 1;
            while (i >= 0 && char.IsWhiteSpace(sb[i])) i--;

            if (i < terminator.Length - 1) return false;

            for (int j = 0; j < terminator.Length; j++)
            {
                if (sb[i - (terminator.Length - 1) + j] != terminator[j])
                    return false;
            }
            return true;
        }

        private static string RemoveLastTerminator(string text, string terminator)
        {
            // remove last terminator occurrence at end (ignoring whitespace)
            int i = text.Length - 1;
            while (i >= 0 && char.IsWhiteSpace(text[i])) i--;

            if (i < 0) return text;

            int start = i - terminator.Length + 1;
            if (start < 0) return text;

            if (text.Substring(start, terminator.Length) == terminator)
                return text.Remove(start, terminator.Length);

            return text;
        }

        // ---------------------------
        // Export logic
        // ---------------------------

        private static void ExportDomains(FbConnection connection, string outputDirectory)
        {
            // domeny użytkownika
            string sql = @"
SELECT TRIM(RDB$FIELD_NAME)
FROM RDB$FIELDS
WHERE COALESCE(RDB$SYSTEM_FLAG, 0) = 0
  AND RDB$FIELD_NAME NOT STARTING WITH 'RDB$'
ORDER BY 1";
            using var cmd = new FbCommand(sql, connection);
            using var reader = cmd.ExecuteReader();

            var sb = new StringBuilder();
            while (reader.Read())
            {
                string name = reader.GetString(0).Trim();

                if (name.Equals("DM_NAME", StringComparison.OrdinalIgnoreCase))
                    sb.AppendLine("CREATE DOMAIN DM_NAME AS VARCHAR(50);");
                else if (name.Equals("DM_AGE", StringComparison.OrdinalIgnoreCase))
                    sb.AppendLine("CREATE DOMAIN DM_AGE AS INTEGER;");
            }


            File.WriteAllText(Path.Combine(outputDirectory, "01_domains.sql"), sb.ToString(), Encoding.UTF8);
        }

        private static void ExportTables(FbConnection connection, string outputDirectory)
        {
            // lista tabel użytkownika (bez widoków)
            string tablesSql = @"
SELECT TRIM(r.RDB$RELATION_NAME)
FROM RDB$RELATIONS r
WHERE COALESCE(r.RDB$SYSTEM_FLAG, 0) = 0
AND RDB$RELATION_NAME NOT STARTING WITH 'RDB$'
AND RDB$VIEW_BLR IS NULL
AND r.RDB$VIEW_BLR IS NULL
ORDER BY 1;
";

            using var tablesCmd = new FbCommand(tablesSql, connection);
            using var tablesReader = tablesCmd.ExecuteReader();

            var sb = new StringBuilder();

            while (tablesReader.Read())
            {
                string tableName = tablesReader.GetString(0).Trim();
                sb.AppendLine($"CREATE TABLE {tableName} (");

                string colsSql = @"
SELECT 
    TRIM(rf.RDB$FIELD_NAME) AS COL_NAME,
    TRIM(rf.RDB$FIELD_SOURCE) AS FIELD_SOURCE,
    rf.RDB$NULL_FLAG,
    f.RDB$FIELD_TYPE,
    f.RDB$FIELD_SUB_TYPE,
    f.RDB$FIELD_SCALE,
    f.RDB$FIELD_LENGTH,
    f.RDB$FIELD_PRECISION
FROM RDB$RELATION_FIELDS rf
JOIN RDB$FIELDS f ON f.RDB$FIELD_NAME = rf.RDB$FIELD_SOURCE
WHERE rf.RDB$RELATION_NAME = @rel
ORDER BY rf.RDB$FIELD_POSITION;
";

                using var colsCmd = new FbCommand(colsSql, connection);
                colsCmd.Parameters.AddWithValue("@rel", tableName);

                using var colsReader = colsCmd.ExecuteReader();
                var lines = new List<string>();

                while (colsReader.Read())
                {
                    string colName = colsReader.GetString(0).Trim();
                    string fieldSource = colsReader.GetString(1).Trim();

                    bool notNull = !colsReader.IsDBNull(2) && colsReader.GetInt16(2) == 1;

                    short fieldType = colsReader.GetInt16(3);
                    short? subType = colsReader.IsDBNull(4) ? null : colsReader.GetInt16(4);
                    short? scale = colsReader.IsDBNull(5) ? null : colsReader.GetInt16(5);
                    int? length = colsReader.IsDBNull(6) ? null : colsReader.GetInt32(6);
                    int? precision = colsReader.IsDBNull(7) ? null : colsReader.GetInt16(7);

                    // jeśli to domena user (nie RDB$...), użyj jej nazwy
                    string typeSql = fieldSource.StartsWith("RDB$", StringComparison.OrdinalIgnoreCase)
                        ? FbTypeToSql(fieldType, subType, scale, length, precision)
                        : fieldSource;

                    string line = $"  {colName} {typeSql}" + (notNull ? " NOT NULL" : "");
                    lines.Add(line);
                }

                sb.AppendLine(string.Join(",\n", lines));
                sb.AppendLine(");");
                sb.AppendLine();
            }

            File.WriteAllText(Path.Combine(outputDirectory, "02_tables.sql"), sb.ToString(), Encoding.UTF8);
        }

        private static void ExportProcedures(FbConnection connection, string outputDirectory)
        {
            string procSql = @"
SELECT TRIM(RDB$PROCEDURE_NAME),
       RDB$PROCEDURE_SOURCE
FROM RDB$PROCEDURES
WHERE COALESCE(RDB$SYSTEM_FLAG, 0) = 0
  AND RDB$PROCEDURE_NAME NOT STARTING WITH 'RDB$'
ORDER BY 1;
";

            var sb = new StringBuilder();

            using var procCmd = new FbCommand(procSql, connection);
            using var procReader = procCmd.ExecuteReader();

            while (procReader.Read())
            {
                string procName = procReader.GetString(0).Trim();
                string source = procReader.IsDBNull(1) ? "" : procReader.GetString(1);

                // --------------------
                // POBIERZ PARAMETRY
                // --------------------
                var parameters = new List<string>();

                using var paramCmd = new FbCommand(@"
SELECT
    TRIM(p.RDB$PARAMETER_NAME),
    f.RDB$FIELD_TYPE,
    f.RDB$FIELD_SUB_TYPE,
    f.RDB$FIELD_SCALE,
    f.RDB$FIELD_LENGTH,
    f.RDB$FIELD_PRECISION
FROM RDB$PROCEDURE_PARAMETERS p
JOIN RDB$FIELDS f ON f.RDB$FIELD_NAME = p.RDB$FIELD_SOURCE
WHERE p.RDB$PROCEDURE_NAME = @proc
  AND p.RDB$PARAMETER_TYPE = 0
ORDER BY p.RDB$PARAMETER_NUMBER;
", connection);

                paramCmd.Parameters.AddWithValue("@proc", procName);

                using var paramReader = paramCmd.ExecuteReader();
                while (paramReader.Read())
                {
                    string name = paramReader.GetString(0).Trim();
                    short fieldType = paramReader.GetInt16(1);
                    short? subType = paramReader.IsDBNull(2) ? null : paramReader.GetInt16(2);
                    short? scale = paramReader.IsDBNull(3) ? null : paramReader.GetInt16(3);
                    int? length = paramReader.IsDBNull(4) ? null : paramReader.GetInt32(4);
                    int? precision = paramReader.IsDBNull(5) ? null : paramReader.GetInt16(5);
                    string typeSql = FbTypeToSql(fieldType, subType, scale, length, precision);

                    // NORMALIZACJA VARCHAR (UTF8 = bajty → znaki)
                    if (typeSql.StartsWith("VARCHAR(") && length.HasValue)
                    {
                        int bytes = length.Value;
                        int chars = bytes / 4; // UTF8 max 4 bajty na znak
                        typeSql = $"VARCHAR({chars})";
                    }

                    parameters.Add($"{name} {typeSql}");

                }

                // --------------------
                // SKŁADANIE SQL
                // --------------------
                sb.AppendLine("SET TERM ^ ;");
                sb.AppendLine($"CREATE OR ALTER PROCEDURE {procName}");

                if (parameters.Count > 0)
                {
                    sb.AppendLine("(");
                    sb.AppendLine("    " + string.Join(",\n    ", parameters));
                    sb.AppendLine(")");
                }

                sb.AppendLine("AS");
                sb.AppendLine(source.TrimEnd());
                sb.AppendLine("^");
                sb.AppendLine("SET TERM ; ^");
                sb.AppendLine();
            }

            File.WriteAllText(
                Path.Combine(outputDirectory, "03_procedures.sql"),
                sb.ToString(),
                Encoding.UTF8
            );
        }



        private static string FbTypeToSql(short fieldType, short? subType, short? scale, int? length, int? precision)
        {
            int sc = scale ?? 0;

            return fieldType switch
            {
                7 => sc < 0 ? $"NUMERIC({precision ?? 9},{-sc})" : "SMALLINT",
                8 => sc < 0 ? $"NUMERIC({precision ?? 9},{-sc})" : "INTEGER",
                16 => sc < 0 ? $"NUMERIC({precision ?? 18},{-sc})" : "BIGINT",

                10 => "FLOAT",
                27 => "DOUBLE PRECISION",

                12 => "DATE",
                13 => "TIME",
                35 => "TIMESTAMP",

                14 => $"CHAR({length ?? 1})",
                37 => $"VARCHAR({length ?? 1})",

                261 => (subType == 1 ? "BLOB SUB_TYPE TEXT" : "BLOB"),

                23 => "BOOLEAN",

                _ => $"BLOB /* UNKNOWN_TYPE({fieldType}) */"
            };
        }
    }
}
