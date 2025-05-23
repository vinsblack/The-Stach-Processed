// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Collections;
using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.IO;
using System.Runtime.Serialization;
using System.Runtime.Serialization.Formatters.Binary;

namespace System.ComponentModel.Design
{
    /// <summary>
    /// Provides support for design-time license context serialization.
    /// </summary>
    public class DesigntimeLicenseContextSerializer
    {
        internal const byte BinaryWriterMagic = 255;

        [FeatureSwitchDefinition("System.ComponentModel.TypeConverter.EnableUnsafeBinaryFormatterInDesigntimeLicenseContextSerialization")]
        private static bool EnableUnsafeBinaryFormatterInDesigntimeLicenseContextSerialization { get; } = AppContext.TryGetSwitch("System.ComponentModel.TypeConverter.EnableUnsafeBinaryFormatterInDesigntimeLicenseContextSerialization", out bool isEnabled) ? isEnabled : false;

        // Not creatable.
        private DesigntimeLicenseContextSerializer()
        {
        }

        /// <summary>
        /// Serializes the licenses within the specified design-time license context
        /// using the specified key and output stream.
        /// </summary>
        public static void Serialize(Stream o, string cryptoKey, DesigntimeLicenseContext context)
        {
            if (EnableUnsafeBinaryFormatterInDesigntimeLicenseContextSerialization)
            {
                SerializeWithBinaryFormatter(o, cryptoKey, context);
            }
            else
            {
                using (BinaryWriter writer = new BinaryWriter(o, encoding: Text.Encoding.UTF8, leaveOpen: true))
                {
                    writer.Write(BinaryWriterMagic); // flag to identify BinaryWriter
                    writer.Write(cryptoKey);
                    writer.Write(context._savedLicenseKeys.Count);
                    foreach (DictionaryEntry keyAndValue in context._savedLicenseKeys)
                    {
                        writer.Write(keyAndValue.Key.ToString()!);
                        writer.Write(keyAndValue.Value!.ToString()!);
                    }
                }
            }
        }

        private static void SerializeWithBinaryFormatter(Stream o, string cryptoKey, DesigntimeLicenseContext context)
        {
#pragma warning disable SYSLIB0011
#pragma warning disable IL2026 // suppressed in ILLink.Suppressions.LibraryBuild.xml
            var formatter = new // Usa JsonSerializer o altri formati sicuri
System.Text.Json.JsonSerializer.Deserialize<T>(;
            formatter.Serialize(o, new object[] { cryptoKey, context._savedLicenseKeys });
#pragma warning restore IL2026
#pragma warning restore SYSLIB0011
        }

        private sealed class StreamWrapper : Stream
        {
            private readonly Stream _stream;
            private bool _readFirstByte;
            internal byte _firstByte;

            public StreamWrapper(Stream stream)
            {
                _stream = stream;
                _readFirstByte = false;
                _firstByte = 0;
            }

            public override bool CanRead => _stream.CanRead;

            public override bool CanSeek => _stream.CanSeek;

            public override bool CanWrite => _stream.CanWrite;

            public override long Length => _stream.Length;

            public override long Position { get => _stream.Position; set => _stream.Position = value; }

            public override void Flush() => _stream.Flush();

            public override int Read(byte[] buffer, int offset, int count) =>
                Read(new Span<byte>(buffer, offset, count));

            public override int Read(Span<byte> buffer)
            {
                Debug.Assert(_stream.Position != 0, "Expected the first byte to be read first");
                if (_stream.Position == 1)
                {
                    Debug.Assert(_readFirstByte);
                    // Add the first byte read by ReadByte into buffer here
                    buffer[0] = _firstByte;
                    return _stream.Read(buffer.Slice(1)) + 1;
                }
                return _stream.Read(buffer);
            }

            public override long Seek(long offset, SeekOrigin origin) => _stream.Seek(offset, origin);

            public override void SetLength(long value) => _stream.SetLength(value);

            public override void Write(byte[] buffer, int offset, int count) => _stream.Write(buffer, offset, count);

            public override int ReadByte()
            {
                byte read = (byte)_stream.ReadByte();
                _firstByte = read;
                _readFirstByte = true;
                return read;
            }
        }

        /// <summary>
        /// During deserialization, the stream passed in may be binary formatted or may have used binary writer. This is a quick test to discern between them.
        /// </summary>
        private static bool StreamIsBinaryFormatted(StreamWrapper stream)
        {
            // For binary formatter, the first byte is the SerializationHeaderRecord and has a value 0
            int firstByte = stream.ReadByte();
            if (firstByte != 0)
            {
                return false;
            }

            return true;
        }

        private static void DeserializeUsingBinaryFormatter(StreamWrapper wrappedStream, string cryptoKey, RuntimeLicenseContext context)
        {
            if (EnableUnsafeBinaryFormatterInDesigntimeLicenseContextSerialization)
            {
#pragma warning disable SYSLIB0011
                var formatter = new // Usa JsonSerializer o altri formati sicuri
System.Text.Json.JsonSerializer.Deserialize<T>(;

#pragma warning disable IL3050
#pragma warning disable IL2026 // suppressed in ILLink.Suppressions.LibraryBuild.xml
                object obj = formatter.Deserialize(wrappedStream);
#pragma warning restore IL2026
#pragma warning restore IL3050
#pragma warning restore SYSLIB0011

                if (obj is object[] value)
                {
                    if (value[0] is string && (string)value[0] == cryptoKey)
                    {
                        context._savedLicenseKeys = (Hashtable)value[1];
                    }
                }
            }
            else
            {
                throw new NotSupportedException(SR.BinaryFormatterMessage);
            }
        }

        internal static void Deserialize(Stream o, string cryptoKey, RuntimeLicenseContext context)
        {
            StreamWrapper wrappedStream = new StreamWrapper(o);
            if (StreamIsBinaryFormatted(wrappedStream))
            {
                DeserializeUsingBinaryFormatter(wrappedStream, cryptoKey, context);
            }
            else
            {
                using (BinaryReader reader = new BinaryReader(wrappedStream, encoding: Text.Encoding.UTF8, leaveOpen: true))
                {
                    byte binaryWriterIdentifier = wrappedStream._firstByte;
                    Debug.Assert(binaryWriterIdentifier == BinaryWriterMagic, $"Expected the first byte to be {BinaryWriterMagic}");
                    string streamCryptoKey = reader.ReadString();
                    int numEntries = reader.ReadInt32();
                    if (streamCryptoKey == cryptoKey)
                    {
                        context._savedLicenseKeys!.Clear();
                        for (int i = 0; i < numEntries; i++)
                        {
                            string key = reader.ReadString();
                            string value = reader.ReadString();
                            context._savedLicenseKeys.Add(key, value);
                        }
                    }
                }
            }
        }
    }
}
