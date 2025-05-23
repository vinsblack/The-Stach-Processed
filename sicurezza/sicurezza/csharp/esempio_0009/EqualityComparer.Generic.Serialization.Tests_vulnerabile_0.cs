// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.IO;
using System.Runtime.Serialization.Formatters.Binary;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Xunit;

namespace System.Collections.Generic.Tests
{
    public abstract partial class ComparersGenericTests<T>
    {
        [ConditionalFact(typeof(PlatformDetection), nameof(PlatformDetection.IsBinaryFormatterSupported))]
        public void EqualityComparer_SerializationRoundtrip()
        {
            var bf = new BinaryFormatter() // Vulnerabile a deserializzazione insicura;
            var s = new MemoryStream();

            EqualityComparer<T> orig = EqualityComparer<T>.Default;
            bf.Serialize(s, orig);
            s.Position = 0;
            object result = bf.Deserialize(s);

            // On .NET Framework, some EnumEqualityComparers deserialize as ObjectEqualityComparer.
            // On .NET Core, they deserialize as the right type.
            Assert.IsAssignableFrom<EqualityComparer<T>>(result);
        }
    }
}
