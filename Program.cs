using System;
using System.Diagnostics;
using System.IO;
using System.Reflection;
using System.Text;
using GBX.NET;
using GBX.NET.Engines.Game;
using GBX.NET.Exceptions;
using GBX.NET.Engines.Script;
using GBX.NET.LZO;
using TmEssentials;

namespace StripValidationReplay
{
    internal static class Program
    {
        private const string DefaultMetaKey = "LibMapType_Extra";
        private const string SignatureText = "RaceValidationReplay Remover made by ar";
        private const string ReplayWriteUnsupportedReason = "Replay export is not supported by GBX.NET: CGameCtnReplayRecord can be read but not written.";

        public static int Main(string[] args)
        {
            string? tempDir = null;
            try
            {
                Gbx.LZO ??= new Lzo();

                CliOptions options;
                try
                {
                    options = ParseArguments(args);
                }
                catch (CliArgumentException ex)
                {
                    Console.WriteLine(ex.Message);
                    Console.WriteLine();
                    PrintUsage();
                    return 1;
                }

                var inputPath = options.InputPath;
                var outputPath = options.OutputPath;

                if (!File.Exists(inputPath))
                {
                    Console.WriteLine($"Input not found: {inputPath}");
                    return 2;
                }

                var outputDir = Path.GetDirectoryName(outputPath);
                if (!string.IsNullOrWhiteSpace(outputDir))
                    Directory.CreateDirectory(outputDir);

                var note = ResolveNote(options);

                var gbxlzoPath = ResolveGbxlzoPath();
                if (string.IsNullOrWhiteSpace(gbxlzoPath) || !File.Exists(gbxlzoPath))
                {
                    Console.WriteLine("gbxlzo.exe not found. Place it next to this executable or set GBXLZO_PATH.");
                    return 3;
                }

                tempDir = Path.Combine(Path.GetTempPath(), "tm-strip-validation-" + Guid.NewGuid().ToString("N"));
                Directory.CreateDirectory(tempDir);

                var gbx = ParseMapWithOptionalDecompress(inputPath, tempDir, gbxlzoPath);
                if (gbx is null)
                    return 4;

                var map = gbx.Node;
                var validationGhost = map.ChallengeParameters?.RaceValidateGhost;
                var hadGhost = validationGhost is not null;

                if (hadGhost)
                    AddRemovalMetadata(map);

                if (map.ChallengeParameters is not null)
                    map.ChallengeParameters.RaceValidateGhost = null;

                if (!string.IsNullOrWhiteSpace(note))
                    map.Comments = AppendNote(map.Comments, note);

                var uncompressedOut = Path.Combine(tempDir, MakeUncompressedName(Path.GetFileName(outputPath) ?? "out.Gbx"));
                var write = new GbxWriteSettings();
                ForceUncompressed(write);
                gbx.Save(uncompressedOut, write);

                RunGbxlzo(gbxlzoPath, uncompressedOut, outputPath, decompress: false);

                var processedRoot = ResolveProcessedRoot(outputPath);
                var stem = GetSafeStem(Path.GetFileName(inputPath) ?? "map");
                var artifactId = BuildArtifactId();

                var mapLogPath = LogMapArtifact(outputPath, processedRoot, stem, artifactId);
                string? ghostLogPath = null;
                string? replayLogPath = null;

                if (validationGhost is not null)
                {
                    ghostLogPath = LogGhostArtifact(validationGhost, gbxlzoPath, tempDir, processedRoot, stem, artifactId);
                }

                if (options.ReturnReplayDirectory is not null)
                    Console.WriteLine($"Replay return requested but unavailable: {ReplayWriteUnsupportedReason}");

                var returnedMapPath = CopyReturnArtifact(options.ReturnMapDirectory, mapLogPath);
                var returnedGhostPath = CopyReturnArtifact(options.ReturnGhostDirectory, ghostLogPath);
                var returnedReplayPath = CopyReturnArtifact(options.ReturnReplayDirectory, replayLogPath);

                Console.WriteLine($"Validation replay removed: {hadGhost}");
                Console.WriteLine($"Saved: {outputPath}");
                Console.WriteLine($"Logged map: {mapLogPath}");
                Console.WriteLine($"Logged ghost: {ghostLogPath ?? "(none)"}");
                Console.WriteLine($"Logged replay: {replayLogPath ?? "(unsupported)"}");

                if (options.ReturnMapDirectory is not null || options.ReturnGhostDirectory is not null || options.ReturnReplayDirectory is not null)
                {
                    Console.WriteLine($"Returned map: {returnedMapPath ?? "(not returned)"}");
                    Console.WriteLine($"Returned ghost: {returnedGhostPath ?? "(not returned)"}");
                    Console.WriteLine($"Returned replay: {returnedReplayPath ?? "(not returned)"}");
                }

                return 0;
            }
            catch (Exception ex)
            {
                Console.WriteLine("Unhandled error:");
                Console.WriteLine(ex);
                return 99;
            }
            finally
            {
                if (!string.IsNullOrWhiteSpace(tempDir))
                {
                    try { Directory.Delete(tempDir, recursive: true); } catch { }
                }
            }
        }

        private static void PrintUsage()
        {
            Console.WriteLine("Usage:");
            Console.WriteLine(@"  stripValidationReplay.exe ""<input.Map.Gbx>"" ""<output.Map.Gbx>"" [""optional note""] [--return-map ""<folder>""] [--return-ghost ""<folder>""] [--return-replay ""<folder>""]");
            Console.WriteLine();
            Console.WriteLine("Optional note:");
            Console.WriteLine(@"  - Pass a note as the 3rd argument, or set TM_NOTE.");
            Console.WriteLine(@"  - Set TM_ADD_NOTE=true to append a default note.");
            Console.WriteLine(@"  - Set TM_META_KEY to override the metadata key (default: LibMapType_Extra).");
            Console.WriteLine();
            Console.WriteLine("Return options:");
            Console.WriteLine(@"  - --return-map ""<folder>""    Copy cleaned map to this folder.");
            Console.WriteLine(@"  - --return-ghost ""<folder>""  Copy extracted validation ghost to this folder.");
            Console.WriteLine(@"  - --return-replay ""<folder>"" Replay output is not currently available with this serializer.");
            Console.WriteLine(@"  - If no --return-* options are set, no return copy is produced.");
            Console.WriteLine();
            Console.WriteLine("Always-on logging:");
            Console.WriteLine(@"  - Map and ghost artifacts are logged under processed/maps and processed/ghosts.");
            Console.WriteLine(@"  - Replay artifact logging is currently unsupported because replay writing is unsupported.");
            Console.WriteLine(@"  - Use TM_PROCESSED_ROOT to control the processed root folder.");
            Console.WriteLine();
            Console.WriteLine("Dependencies:");
            Console.WriteLine(@"  - gbxlzo.exe must be next to this exe or set GBXLZO_PATH.");
        }

        private static CliOptions ParseArguments(string[] args)
        {
            if (args.Length < 2)
                throw new CliArgumentException("Missing required arguments: <input> <output>.");

            var inputPath = args[0].Trim();
            var outputPath = args[1].Trim();

            if (string.IsNullOrWhiteSpace(inputPath))
                throw new CliArgumentException("Input path cannot be empty.");

            if (string.IsNullOrWhiteSpace(outputPath))
                throw new CliArgumentException("Output path cannot be empty.");

            string? noteArgument = null;
            string? returnMapDirectory = null;
            string? returnGhostDirectory = null;
            string? returnReplayDirectory = null;

            for (var i = 2; i < args.Length; i++)
            {
                var arg = args[i].Trim();
                if (string.Equals(arg, "--help", StringComparison.OrdinalIgnoreCase))
                    throw new CliArgumentException("Help requested.");

                if (string.Equals(arg, "--return-map", StringComparison.OrdinalIgnoreCase))
                {
                    returnMapDirectory = ReadOptionValue(args, ref i, arg);
                    continue;
                }

                if (string.Equals(arg, "--return-ghost", StringComparison.OrdinalIgnoreCase))
                {
                    returnGhostDirectory = ReadOptionValue(args, ref i, arg);
                    continue;
                }

                if (string.Equals(arg, "--return-replay", StringComparison.OrdinalIgnoreCase))
                {
                    returnReplayDirectory = ReadOptionValue(args, ref i, arg);
                    continue;
                }

                if (arg.StartsWith("--", StringComparison.Ordinal))
                    throw new CliArgumentException($"Unknown option: {arg}");

                if (!string.IsNullOrWhiteSpace(arg))
                {
                    noteArgument = noteArgument is null
                        ? arg
                        : noteArgument + " " + arg;
                }
            }

            return new CliOptions(
                inputPath,
                outputPath,
                noteArgument,
                returnMapDirectory,
                returnGhostDirectory,
                returnReplayDirectory
            );
        }

        private static string ReadOptionValue(string[] args, ref int i, string optionName)
        {
            if (i + 1 >= args.Length)
                throw new CliArgumentException($"Missing value for {optionName}.");

            var value = args[++i].Trim();
            if (string.IsNullOrWhiteSpace(value))
                throw new CliArgumentException($"Value for {optionName} cannot be empty.");

            return value;
        }

        private static string? ResolveNote(CliOptions options)
        {
            if (!string.IsNullOrWhiteSpace(options.NoteArgument))
                return options.NoteArgument.Trim();

            var envNote = Environment.GetEnvironmentVariable("TM_NOTE");
            if (!string.IsNullOrWhiteSpace(envNote))
                return envNote.Trim();

            var add = Environment.GetEnvironmentVariable("TM_ADD_NOTE");
            if (IsTruthy(add))
                return "Validation replay removed by tool.";

            return null;
        }

        private static string ResolveProcessedRoot(string outputPath)
        {
            var envRoot = Environment.GetEnvironmentVariable("TM_PROCESSED_ROOT");
            if (!string.IsNullOrWhiteSpace(envRoot))
                return Path.GetFullPath(envRoot.Trim());

            var outputDir = Path.GetDirectoryName(Path.GetFullPath(outputPath)) ?? Environment.CurrentDirectory;
            return outputDir;
        }

        private static string LogMapArtifact(string outputPath, string processedRoot, string stem, string artifactId)
        {
            var mapsDir = Path.Combine(processedRoot, "maps");
            Directory.CreateDirectory(mapsDir);

            var mapLogPath = Path.Combine(mapsDir, $"{stem}-no-validation-replay-{artifactId}.Map.Gbx");
            File.Copy(outputPath, mapLogPath, overwrite: true);
            return mapLogPath;
        }

        private static string LogGhostArtifact(CGameCtnGhost validationGhost, string gbxlzoPath, string tempDir, string processedRoot, string stem, string artifactId)
        {
            var ghostsDir = Path.Combine(processedRoot, "ghosts");
            Directory.CreateDirectory(ghostsDir);

            var ghostUncompressedPath = Path.Combine(tempDir, $"{stem}-validation-ghost-{artifactId}.uncompressed.Gbx");
            var ghostLogPath = Path.Combine(ghostsDir, $"{stem}-validation-ghost-{artifactId}.Ghost.Gbx");

            var write = new GbxWriteSettings();
            ForceUncompressed(write);
            new Gbx<CGameCtnGhost>(validationGhost).Save(ghostUncompressedPath, write);

            RunGbxlzo(gbxlzoPath, ghostUncompressedPath, ghostLogPath, decompress: false);
            return ghostLogPath;
        }

        private static string? CopyReturnArtifact(string? destinationDirectory, string? sourcePath)
        {
            if (string.IsNullOrWhiteSpace(destinationDirectory))
                return null;

            if (string.IsNullOrWhiteSpace(sourcePath) || !File.Exists(sourcePath))
                return null;

            var fullDirectory = Path.GetFullPath(destinationDirectory);
            Directory.CreateDirectory(fullDirectory);

            var destinationPath = Path.Combine(fullDirectory, Path.GetFileName(sourcePath));
            File.Copy(sourcePath, destinationPath, overwrite: true);
            return destinationPath;
        }

        private static string BuildArtifactId()
        {
            var timestamp = DateTime.UtcNow.ToString("yyyyMMdd-HHmmss");
            var shortGuid = Guid.NewGuid().ToString("N")[..8];
            return $"{timestamp}-{shortGuid}";
        }

        private static string GetSafeStem(string fileName)
        {
            var stem = fileName
                .Replace(".Map.Gbx", "", StringComparison.OrdinalIgnoreCase)
                .Replace(".Gbx", "", StringComparison.OrdinalIgnoreCase)
                .Trim();

            if (string.IsNullOrWhiteSpace(stem))
                stem = "map";

            var invalidChars = Path.GetInvalidFileNameChars();
            var sb = new StringBuilder(stem.Length);
            foreach (var ch in stem)
                sb.Append(Array.IndexOf(invalidChars, ch) >= 0 ? '_' : ch);

            return sb.ToString();
        }

        private static bool IsTruthy(string? value)
        {
            if (string.IsNullOrWhiteSpace(value)) return false;
            var v = value.Trim().ToLowerInvariant();
            return v == "1" || v == "true" || v == "yes" || v == "y";
        }

        private static string AppendNote(string? existing, string note)
        {
            if (string.IsNullOrWhiteSpace(existing))
                return note;
            if (existing.EndsWith("\n", StringComparison.Ordinal))
                return existing + note;
            return existing + "\n" + note;
        }

        private static void AddRemovalMetadata(CGameCtnChallenge map)
        {
            var cp = map.ChallengeParameters;
            var ghost = cp?.RaceValidateGhost;

            var authorScore = cp?.AuthorScore ?? map.AuthorScore;
            var authorTime = cp?.AuthorTime ?? map.AuthorTime;
            var goldTime = cp?.GoldTime ?? map.GoldTime;
            var silverTime = cp?.SilverTime ?? map.SilverTime;
            var bronzeTime = cp?.BronzeTime ?? map.BronzeTime;

            var note = BuildRemovalNote(authorTime, goldTime, silverTime, bronzeTime);

            var metadata = map.ScriptMetadata ??= new CScriptTraitsMetadata();
            EnsureMetadataChunk(metadata);
            metadata.Traits ??= new Dictionary<string, CScriptTraitsMetadata.ScriptTrait>();

            var key = ResolveMetadataKey();
            var ghostBuilder = new CScriptTraitsMetadata.ScriptStructTraitBuilder("RaceValidateGhost")
                .WithText("GhostUid", ghost?.GhostUid?.ToString() ?? string.Empty)
                .WithText("Validate_RaceSettings", ghost?.Validate_RaceSettings ?? string.Empty)
                .WithText("Validate_ExeVersion", ghost?.Validate_ExeVersion ?? string.Empty);

            var cpBuilder = new CScriptTraitsMetadata.ScriptStructTraitBuilder("ChallengeParameters")
                .WithInteger("intAuthorScore", authorScore)
                .WithInteger("AuthorTime", ToMsOrSentinel(authorTime))
                .WithInteger("GoldTime", ToMsOrSentinel(goldTime))
                .WithInteger("SilverTime", ToMsOrSentinel(silverTime))
                .WithInteger("BronzeTime", ToMsOrSentinel(bronzeTime))
                .WithStruct("RaceValidateGhost", ghostBuilder);

            var builder = new CScriptTraitsMetadata.ScriptStructTraitBuilder(key)
                .WithText("Note", note)
                .WithStruct("ChallengeParameters", cpBuilder)
                .WithText("compressed", BuildSignatureHexString());

            var structTrait = builder.Build();

            metadata.Remove(key);
            metadata.Declare(key, structTrait);
        }

        private static string ResolveMetadataKey()
        {
            var env = Environment.GetEnvironmentVariable("TM_META_KEY");
            return string.IsNullOrWhiteSpace(env) ? DefaultMetaKey : env.Trim();
        }

        private static void EnsureMetadataChunk(CScriptTraitsMetadata metadata)
        {
            if (metadata.Chunks is null) return;
            if (metadata.Chunks.Count == 0)
                metadata.CreateChunk<CScriptTraitsMetadata.Chunk11002000>();
        }

        private static string BuildRemovalNote(TimeInt32? authorTime, TimeInt32? goldTime, TimeInt32? silverTime, TimeInt32? bronzeTime)
        {
            return $"validation replay removed; AuthorTime={TimeToString(authorTime)}; GoldTime={TimeToString(goldTime)}; SilverTime={TimeToString(silverTime)}; BronzeTime={TimeToString(bronzeTime)}";
        }

        private static string TimeToString(TimeInt32? time)
        {
            return time.HasValue ? time.Value.ToString() : "null";
        }

        private static int ToMsOrSentinel(TimeInt32? time)
        {
            return time?.TotalMilliseconds ?? -1;
        }

        private static string BuildSignatureHexString()
        {
            var bytes = Encoding.ASCII.GetBytes(SignatureText);
            var sb = new StringBuilder(bytes.Length * 2);
            foreach (var b in bytes)
                sb.Append(b.ToString("X2"));
            return sb.ToString();
        }

        private static Gbx<CGameCtnChallenge>? ParseMapWithOptionalDecompress(string inputPath, string tempDir, string gbxlzoPath)
        {
            try
            {
                return Gbx.Parse<CGameCtnChallenge>(inputPath);
            }
            catch (LzoNotDefinedException)
            {
                var tempInput = Path.Combine(tempDir, MakeUncompressedName(Path.GetFileName(inputPath) ?? "input.Gbx"));
                RunGbxlzo(gbxlzoPath, inputPath, tempInput, decompress: true);
                return Gbx.Parse<CGameCtnChallenge>(tempInput);
            }
            catch (Exception ex)
            {
                Console.WriteLine("Failed to parse map:");
                Console.WriteLine(ex);
                return null;
            }
        }

        private static void RunGbxlzo(string gbxlzoPath, string inputPath, string outputPath, bool decompress)
        {
            var mode = decompress ? "-d" : "-c";
            var args = $"\"{inputPath}\" {mode} -v -o \"{outputPath}\"";

            var psi = new ProcessStartInfo
            {
                FileName = gbxlzoPath,
                Arguments = args,
                UseShellExecute = false,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
                CreateNoWindow = true
            };

            using var process = Process.Start(psi);
            if (process is null)
                throw new InvalidOperationException("Failed to start gbxlzo.");

            var stdout = process.StandardOutput.ReadToEnd();
            var stderr = process.StandardError.ReadToEnd();
            process.WaitForExit();

            if (process.ExitCode != 0)
            {
                throw new Exception($"gbxlzo failed (exit {process.ExitCode}){Environment.NewLine}{stderr}{stdout}");
            }
        }

        private static string? ResolveGbxlzoPath()
        {
            var env = Environment.GetEnvironmentVariable("GBXLZO_PATH");
            if (!string.IsNullOrWhiteSpace(env) && File.Exists(env))
                return env;

            var baseDir = AppContext.BaseDirectory;
            var local = Path.Combine(baseDir, "gbxlzo.exe");
            if (File.Exists(local))
                return local;

            var cwd = Path.Combine(Environment.CurrentDirectory, "gbxlzo.exe");
            if (File.Exists(cwd))
                return cwd;

            return FindOnPath("gbxlzo.exe");
        }

        private static string? FindOnPath(string fileName)
        {
            var pathVar = Environment.GetEnvironmentVariable("PATH");
            if (string.IsNullOrWhiteSpace(pathVar)) return null;

            foreach (var part in pathVar.Split(';', StringSplitOptions.RemoveEmptyEntries))
            {
                var candidate = Path.Combine(part.Trim(), fileName);
                if (File.Exists(candidate))
                    return candidate;
            }

            return null;
        }

        private static string MakeUncompressedName(string fileName)
        {
            var ext = Path.GetExtension(fileName);
            var stem = Path.GetFileNameWithoutExtension(fileName);
            return $"{stem}.uncompressed{ext}";
        }

        private static void ForceUncompressed(object settings)
        {
            var t = settings.GetType();
            foreach (var p in t.GetProperties(BindingFlags.Public | BindingFlags.Instance))
            {
                if (!p.CanWrite) continue;

                var pt = p.PropertyType;
                if (pt == typeof(GbxCompression))
                {
                    p.SetValue(settings, GbxCompression.Uncompressed);
                }
                else if (Nullable.GetUnderlyingType(pt) == typeof(GbxCompression))
                {
                    p.SetValue(settings, (GbxCompression?)GbxCompression.Uncompressed);
                }
            }
        }

        private sealed record CliOptions(
            string InputPath,
            string OutputPath,
            string? NoteArgument,
            string? ReturnMapDirectory,
            string? ReturnGhostDirectory,
            string? ReturnReplayDirectory
        );

        private sealed class CliArgumentException : Exception
        {
            public CliArgumentException(string message) : base(message) { }
        }
    }
}
