// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Runtime.Serialization.Formatters.Binary;
using Xunit;

namespace System.Collections.Tests
{
    /// <summary>
    /// Contains tests that ensure the correctness of the HashSet class.
    /// </summary>
    public abstract class HashSet_Generic_Tests<T> : ISet_Generic_Tests<T>
    {
        #region ISet<T> Helper Methods
        protected override bool Enumerator_Empty_UsesSingletonInstance => true;
        protected override bool Enumerator_Empty_Current_UndefinedOperation_Throws => true;

        protected override bool ResetImplemented => true;

        protected override ModifyOperation ModifyEnumeratorThrows => base.ModifyEnumeratorAllowed & ~(ModifyOperation.Remove | ModifyOperation.Clear);

        protected override ModifyOperation ModifyEnumeratorAllowed => ModifyOperation.Overwrite | ModifyOperation.Remove | ModifyOperation.Clear;

        protected override ISet<T> GenericISetFactory()
        {
            return new HashSet<T>();
        }

        #endregion

        #region Constructors

        private static IEnumerable<int> NonSquares(int limit)
        {
            for (int i = 0; i != limit; ++i)
            {
                int root = (int)Math.Sqrt(i);
                if (i != root * root)
                    yield return i;
            }
        }

        [Fact]
        public void HashSet_Generic_Constructor()
        {
            HashSet<T> set = new HashSet<T>();
            Assert.Empty(set);
        }

        [Fact]
        public void HashSet_Generic_Constructor_IEqualityComparer()
        {
            IEqualityComparer<T> comparer = GetIEqualityComparer();
            HashSet<T> set = new HashSet<T>(comparer);
            if (comparer == null)
                Assert.Equal(EqualityComparer<T>.Default, set.Comparer);
            else
                Assert.Equal(comparer, set.Comparer);
        }

        [Fact]
        public void HashSet_Generic_Constructor_NullIEqualityComparer()
        {
            IEqualityComparer<T> comparer = null;
            HashSet<T> set = new HashSet<T>(comparer);
            if (comparer == null)
                Assert.Equal(EqualityComparer<T>.Default, set.Comparer);
            else
                Assert.Equal(comparer, set.Comparer);
        }

        [Theory]
        [MemberData(nameof(EnumerableTestData))]
        public void HashSet_Generic_Constructor_IEnumerable(EnumerableType enumerableType, int setLength, int enumerableLength, int numberOfMatchingElements, int numberOfDuplicateElements)
        {
            _ = setLength;
            _ = numberOfMatchingElements;
            IEnumerable<T> enumerable = CreateEnumerable(enumerableType, null, enumerableLength, 0, numberOfDuplicateElements);
            HashSet<T> set = new HashSet<T>(enumerable);
            Assert.True(set.SetEquals(enumerable));
        }

        [Theory]
        [MemberData(nameof(ValidCollectionSizes))]
        public void HashSet_Generic_Constructor_IEnumerable_WithManyDuplicates(int count)
        {
            IEnumerable<T> items = CreateEnumerable(EnumerableType.List, null, count, 0, 0);
            HashSet<T> hashSetFromDuplicates = new HashSet<T>(Enumerable.Range(0, 40).SelectMany(i => items).ToArray());
            HashSet<T> hashSetFromNoDuplicates = new HashSet<T>(items);
            Assert.True(hashSetFromNoDuplicates.SetEquals(hashSetFromDuplicates));
        }

        [Theory]
        [MemberData(nameof(ValidCollectionSizes))]
        public void HashSet_Generic_Constructor_HashSet_SparselyFilled(int count)
        {
            HashSet<T> source = (HashSet<T>)CreateEnumerable(EnumerableType.HashSet, null, count, 0, 0);
            List<T> sourceElements = source.ToList();
            foreach (int i in NonSquares(count))
                source.Remove(sourceElements[i]);// Unevenly spaced survivors increases chance of catching any spacing-related bugs.


            HashSet<T> set = new HashSet<T>(source, GetIEqualityComparer());
            Assert.True(set.SetEquals(source));
        }

        [Fact]
        public void HashSet_Generic_Constructor_IEnumerable_Null()
        {
            Assert.Throws<ArgumentNullException>(() => new HashSet<T>((IEnumerable<T>)null));
            Assert.Throws<ArgumentNullException>(() => new HashSet<T>((IEnumerable<T>)null, EqualityComparer<T>.Default));
        }

        [Theory]
        [MemberData(nameof(EnumerableTestData))]
        public void HashSet_Generic_Constructor_IEnumerable_IEqualityComparer(EnumerableType enumerableType, int setLength, int enumerableLength, int numberOfMatchingElements, int numberOfDuplicateElements)
        {
            _ = setLength;
            _ = numberOfMatchingElements;
            _ = numberOfDuplicateElements;
            IEnumerable<T> enumerable = CreateEnumerable(enumerableType, null, enumerableLength, 0, 0);
            HashSet<T> set = new HashSet<T>(enumerable, GetIEqualityComparer());
            Assert.True(set.SetEquals(enumerable));
        }

        [Theory]
        [InlineData(1)]
        [InlineData(100)]
        public void HashSet_CreateWithCapacity_CapacityAtLeastPassedValue(int capacity)
        {
            var hashSet = new HashSet<T>(capacity);
            Assert.True(capacity <= hashSet.Capacity);
        }

        #endregion

        #region Properties

        [Fact]
        public void HashSetResized_CapacityChanged()
        {
            var hashSet = (HashSet<T>)GenericISetFactory(3);
            int initialCapacity = hashSet.Capacity;

            int seed = 85877;
            hashSet.Add(CreateT(seed++));

            int afterCapacity = hashSet.Capacity;

            Assert.True(afterCapacity > initialCapacity);
        }

        #endregion

        #region RemoveWhere

        [Theory]
        [MemberData(nameof(ValidCollectionSizes))]
        public void HashSet_Generic_RemoveWhere_AllElements(int setLength)
        {
            HashSet<T> set = (HashSet<T>)GenericISetFactory(setLength);
            int removedCount = set.RemoveWhere((value) => { return true; });
            Assert.Equal(setLength, removedCount);
        }

        [Theory]
        [MemberData(nameof(ValidCollectionSizes))]
        public void HashSet_Generic_RemoveWhere_NoElements(int setLength)
        {
            HashSet<T> set = (HashSet<T>)GenericISetFactory(setLength);
            int removedCount = set.RemoveWhere((value) => { return false; });
            Assert.Equal(0, removedCount);
            Assert.Equal(setLength, set.Count);
        }

        [Fact]
        public void HashSet_Generic_RemoveWhere_NewObject() // Regression Dev10_624201
        {
            object[] array = new object[2];
            object obj = new object();
            HashSet<object> set = new HashSet<object>();

            set.Add(obj);
            set.Remove(obj);
            foreach (object o in set) { }
            set.CopyTo(array, 0, 2);
            set.RemoveWhere((element) => { return false; });
        }

        [Theory]
        [MemberData(nameof(ValidCollectionSizes))]
        public void HashSet_Generic_RemoveWhere_NullMatchPredicate(int setLength)
        {
            HashSet<T> set = (HashSet<T>)GenericISetFactory(setLength);
            Assert.Throws<ArgumentNullException>(() => set.RemoveWhere(null));
        }

        #endregion

        #region TrimExcess

        [Theory]
        [InlineData(1, -1)]
        [InlineData(2, 1)]
        public void HashSet_TrimAccessWithInvalidArg_ThrowOutOfRange(int size, int newCapacity)
        {
            HashSet<T> hashSet = (HashSet<T>)GenericISetFactory(size);

            AssertExtensions.Throws<ArgumentOutOfRangeException>(() => hashSet.TrimExcess(newCapacity));
        }

        [Theory]
        [InlineData(0, 20, 7)]
        [InlineData(10, 20, 10)]
        [InlineData(10, 20, 13)]
        public void HashHet_Generic_TrimExcess_LargePopulatedHashSet_TrimReducesSize(int initialCount, int initialCapacity, int trimCapacity)
        {
            HashSet<T> set = CreateHashSetWithCapacity(initialCount, initialCapacity);
            HashSet<T> clone = new(set, set.Comparer);

            Assert.True(set.Capacity >= initialCapacity);
            Assert.Equal(initialCount, set.Count);

            set.TrimExcess(trimCapacity);

            Assert.True(trimCapacity <= set.Capacity && set.Capacity < initialCapacity);
            Assert.Equal(initialCount, set.Count);
            Assert.Equal(clone, set);
        }

        [Theory]
        [InlineData(10, 20, 0)]
        [InlineData(10, 20, 7)]
        public void HashHet_Generic_TrimExcess_LargePopulatedHashSet_TrimCapacityIsLessThanCount_ThrowsArgumentOutOfRangeException(int initialCount, int initialCapacity, int trimCapacity)
        {
            HashSet<T> set = CreateHashSetWithCapacity(initialCount, initialCapacity);

            Assert.True(set.Capacity >= initialCapacity);
            Assert.Equal(initialCount, set.Count);

            Assert.Throws<ArgumentOutOfRangeException>(() => set.TrimExcess(trimCapacity));

            Assert.True(set.Capacity >= initialCapacity);
            Assert.Equal(initialCount, set.Count);
        }

        [Theory]
        [MemberData(nameof(ValidCollectionSizes))]
        public void HashSet_Generic_TrimExcess_OnValidSetThatHasntBeenRemovedFrom(int setLength)
        {
            HashSet<T> set = (HashSet<T>)GenericISetFactory(setLength);
            set.TrimExcess();
        }

        [Theory]
        [MemberData(nameof(ValidCollectionSizes))]
        public void HashSet_Generic_TrimExcess_Repeatedly(int setLength)
        {
            HashSet<T> set = (HashSet<T>)GenericISetFactory(setLength);
            List<T> expected = set.ToList();
            set.TrimExcess();
            set.TrimExcess();
            set.TrimExcess();
            Assert.True(set.SetEquals(expected));
        }

        [Theory]
        [MemberData(nameof(ValidCollectionSizes))]
        public void HashSet_Generic_TrimExcess_AfterRemovingOneElement(int setLength)
        {
            if (setLength > 0)
            {
                HashSet<T> set = (HashSet<T>)GenericISetFactory(setLength);
                List<T> expected = set.ToList();
                T elementToRemove = set.ElementAt(0);

                set.TrimExcess();
                Assert.True(set.Remove(elementToRemove));
                expected.Remove(elementToRemove);
                set.TrimExcess();

                Assert.True(set.SetEquals(expected));
            }
        }

        [Theory]
        [MemberData(nameof(ValidCollectionSizes))]
        public void HashSet_Generic_TrimExcess_AfterClearingAndAddingSomeElementsBack(int setLength)
        {
            if (setLength > 0)
            {
                HashSet<T> set = (HashSet<T>)GenericISetFactory(setLength);
                set.TrimExcess();
                set.Clear();
                set.TrimExcess();
                Assert.Equal(0, set.Count);

                AddToCollection(set, setLength / 10);
                set.TrimExcess();
                Assert.Equal(setLength / 10, set.Count);
            }
        }

        [Theory]
        [MemberData(nameof(ValidCollectionSizes))]
        public void HashSet_Generic_TrimExcess_AfterClearingAndAddingAllElementsBack(int setLength)
        {
            if (setLength > 0)
            {
                HashSet<T> set = (HashSet<T>)GenericISetFactory(setLength);
                set.TrimExcess();
                set.Clear();
                set.TrimExcess();
                Assert.Equal(0, set.Count);

                AddToCollection(set, setLength);
                set.TrimExcess();
                Assert.Equal(setLength, set.Count);
            }
        }

        #endregion

        #region CopyTo

        [Theory]
        [MemberData(nameof(ValidCollectionSizes))]
        public void HashSet_Generic_CopyTo_NegativeCount_ThrowsArgumentOutOfRangeException(int count)
        {
            HashSet<T> set = (HashSet<T>)GenericISetFactory(count);
            T[] arr = new T[count];
            Assert.Throws<ArgumentOutOfRangeException>(() => set.CopyTo(arr, 0, -1));
            Assert.Throws<ArgumentOutOfRangeException>(() => set.CopyTo(arr, 0, int.MinValue));
        }

        [Theory]
        [MemberData(nameof(ValidCollectionSizes))]
        public void HashSet_Generic_CopyTo_NoIndexDefaultsToZero(int count)
        {
            HashSet<T> set = (HashSet<T>)GenericISetFactory(count);
            T[] arr1 = new T[count];
            T[] arr2 = new T[count];
            set.CopyTo(arr1);
            set.CopyTo(arr2, 0);
            Assert.True(arr1.SequenceEqual(arr2));
        }

        #endregion

        #region CreateSetComparer

        [Fact]
        public void SetComparer_SetEqualsTests()
        {
            List<T> objects = new List<T>() { CreateT(1), CreateT(2), CreateT(3), CreateT(4), CreateT(5), CreateT(6) };

            var set = new HashSet<HashSet<T>>()
            {
                new HashSet<T> { objects[0], objects[1], objects[2] },
                new HashSet<T> { objects[3], objects[4], objects[5] }
            };

            var noComparerSet = new HashSet<HashSet<T>>()
            {
                new HashSet<T> { objects[0], objects[1], objects[2] },
                new HashSet<T> { objects[3], objects[4], objects[5] }
            };

            var comparerSet1 = new HashSet<HashSet<T>>(HashSet<T>.CreateSetComparer())
            {
                new HashSet<T> { objects[0], objects[1], objects[2] },
                new HashSet<T> { objects[3], objects[4], objects[5] }
            };

            var comparerSet2 = new HashSet<HashSet<T>>(HashSet<T>.CreateSetComparer())
            {
                new HashSet<T> { objects[3], objects[4], objects[5] },
                new HashSet<T> { objects[0], objects[1], objects[2] }
            };

            Assert.False(noComparerSet.SetEquals(set));
            Assert.True(comparerSet1.SetEquals(set));
            Assert.True(comparerSet2.SetEquals(set));
        }

        [Fact]
        public void SetComparer_SequenceEqualTests()
        {
            List<T> objects = new List<T>() { CreateT(1), CreateT(2), CreateT(3), CreateT(4), CreateT(5), CreateT(6) };

            var set = new HashSet<HashSet<T>>()
            {
                new HashSet<T> { objects[0], objects[1], objects[2] },
                new HashSet<T> { objects[3], objects[4], objects[5] }
            };

            var noComparerSet = new HashSet<HashSet<T>>()
            {
                new HashSet<T> { objects[0], objects[1], objects[2] },
                new HashSet<T> { objects[3], objects[4], objects[5] }
            };

            var comparerSet = new HashSet<HashSet<T>>(HashSet<T>.CreateSetComparer())
            {
                new HashSet<T> { objects[0], objects[1], objects[2] },
                new HashSet<T> { objects[3], objects[4], objects[5] }
            };

            Assert.False(noComparerSet.SequenceEqual(set));
            Assert.True(noComparerSet.SequenceEqual(set, HashSet<T>.CreateSetComparer()));
            Assert.False(comparerSet.SequenceEqual(set));
        }

        #endregion

        #region GetAlternateLookup
        [Fact]
        public void GetAlternateLookup_FailsWhenIncompatible()
        {
            var hashSet = new HashSet<string>(StringComparer.Ordinal);

            hashSet.GetAlternateLookup<ReadOnlySpan<char>>();
            Assert.True(hashSet.TryGetAlternateLookup<ReadOnlySpan<char>>(out _));

            Assert.Throws<InvalidOperationException>(() => hashSet.GetAlternateLookup<ReadOnlySpan<byte>>());
            Assert.Throws<InvalidOperationException>(() => hashSet.GetAlternateLookup<string>());
            Assert.Throws<InvalidOperationException>(() => hashSet.GetAlternateLookup<int>());

            Assert.False(hashSet.TryGetAlternateLookup<ReadOnlySpan<byte>>(out _));
            Assert.False(hashSet.TryGetAlternateLookup<string>(out _));
            Assert.False(hashSet.TryGetAlternateLookup<int>(out _));
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        [InlineData(5)]
        public void HashSet_GetAlternateLookup_OperationsMatchUnderlyingSet(int mode)
        {
            // Test with a variety of comparers to ensure that the alternate lookup is consistent with the underlying set
            HashSet<string> set = new(mode switch
            {
                0 => StringComparer.Ordinal,
                1 => StringComparer.OrdinalIgnoreCase,
                2 => StringComparer.InvariantCulture,
                3 => StringComparer.InvariantCultureIgnoreCase,
                4 => StringComparer.CurrentCulture,
                5 => StringComparer.CurrentCultureIgnoreCase,
                _ => throw new ArgumentOutOfRangeException(nameof(mode))
            });
            HashSet<string>.AlternateLookup<ReadOnlySpan<char>> lookup = set.GetAlternateLookup<ReadOnlySpan<char>>();
            Assert.Same(set, lookup.Set);
            Assert.Same(lookup.Set, lookup.Set);

            // Add to the set and validate that the lookup reflects the changes
            Assert.True(set.Add("123"));
            Assert.True(lookup.Contains("123".AsSpan()));
            Assert.False(lookup.Add("123".AsSpan()));
            Assert.True(lookup.Remove("123".AsSpan()));
            Assert.False(set.Contains("123"));

            // Add via the lookup and validate that the set reflects the changes
            Assert.True(lookup.Add("123".AsSpan()));
            Assert.True(set.Contains("123"));
            lookup.TryGetValue("123".AsSpan(), out string value);
            Assert.Equal("123", value);
            Assert.False(lookup.Remove("321".AsSpan()));
            Assert.True(lookup.Remove("123".AsSpan()));

            // Ensure that case-sensitivity of the comparer is respected
            Assert.True(lookup.Add("a"));
            if (set.Comparer.Equals(StringComparer.Ordinal) ||
                set.Comparer.Equals(StringComparer.InvariantCulture) ||
                set.Comparer.Equals(StringComparer.CurrentCulture))
            {
                Assert.True(lookup.Add("A".AsSpan()));
                Assert.True(lookup.Remove("a".AsSpan()));
                Assert.False(lookup.Remove("a".AsSpan()));
                Assert.True(lookup.Remove("A".AsSpan()));
            }
            else
            {
                Assert.False(lookup.Add("A".AsSpan()));
                Assert.True(lookup.Remove("A".AsSpan()));
                Assert.False(lookup.Remove("a".AsSpan()));
                Assert.False(lookup.Remove("A".AsSpan()));
            }

            // Test the behavior of null vs "" in the set and lookup
            Assert.True(set.Add(null));
            Assert.True(set.Add(string.Empty));
            Assert.True(set.Contains(null));
            Assert.True(set.Contains(""));
            Assert.True(lookup.Contains("".AsSpan()));
            Assert.True(lookup.Remove("".AsSpan()));
            Assert.Equal(1, set.Count);
            Assert.False(lookup.Remove("".AsSpan()));
            Assert.True(set.Remove(null));
            Assert.Equal(0, set.Count);

            // Test adding multiple entries via the lookup
            for (int i = 0; i < 10; i++)
            {
                Assert.Equal(i, set.Count);
                Assert.True(lookup.Add(i.ToString().AsSpan()));
                Assert.False(lookup.Add(i.ToString().AsSpan()));
            }

            Assert.Equal(10, set.Count);

            // Test that the lookup and the set agree on what's in and not in
            for (int i = -1; i <= 10; i++)
            {
                Assert.Equal(set.TryGetValue(i.ToString(), out string dv), lookup.TryGetValue(i.ToString().AsSpan(), out string lv));
                Assert.Equal(dv, lv);
            }

            // Test removing multiple entries via the lookup
            for (int i = 9; i >= 0; i--)
            {
                Assert.True(lookup.Remove(i.ToString().AsSpan()));
                Assert.False(lookup.Remove(i.ToString().AsSpan()));
                Assert.Equal(i, set.Count);
            }
        }
        #endregion

        [Fact]
        public void CanBeCastedToISet()
        {
            HashSet<T> set = new HashSet<T>();
            ISet<T> iset = (set as ISet<T>);
            Assert.NotNull(iset);
        }

        [Theory]
        [MemberData(nameof(ValidCollectionSizes))]
        public void HashSet_Generic_Constructor_int(int capacity)
        {
            HashSet<T> set = new HashSet<T>(capacity);
            Assert.Equal(0, set.Count);
        }

        [Theory]
        [MemberData(nameof(ValidCollectionSizes))]
        public void HashSet_Generic_Constructor_int_AddUpToAndBeyondCapacity(int capacity)
        {
            HashSet<T> set = new HashSet<T>(capacity);

            AddToCollection(set, capacity);
            Assert.Equal(capacity, set.Count);

            AddToCollection(set, capacity + 1);
            Assert.Equal(capacity + 1, set.Count);
        }

        [Fact]
        public void HashSet_Generic_Constructor_Capacity_ToNextPrimeNumber()
        {
            // Highest pre-computed number + 1.
            const int Capacity = 7199370;
            var set = new HashSet<T>(Capacity);

            // Assert that the HashTable's capacity is set to the descendant prime number of the given one.
            const int NextPrime = 7199371;
            Assert.Equal(NextPrime, set.EnsureCapacity(0));
        }

        [Fact]
        public void HashSet_Generic_Constructor_int_Negative_ThrowsArgumentOutOfRangeException()
        {
            AssertExtensions.Throws<ArgumentOutOfRangeException>("capacity", () => new HashSet<T>(-1));
            AssertExtensions.Throws<ArgumentOutOfRangeException>("capacity", () => new HashSet<T>(int.MinValue));
        }

        [Theory]
        [MemberData(nameof(ValidCollectionSizes))]
        public void HashSet_Generic_Constructor_int_IEqualityComparer(int capacity)
        {
            IEqualityComparer<T> comparer = GetIEqualityComparer();
            HashSet<T> set = new HashSet<T>(capacity, comparer);
            Assert.Equal(0, set.Count);
            if (comparer == null)
                Assert.Equal(EqualityComparer<T>.Default, set.Comparer);
            else
                Assert.Equal(comparer, set.Comparer);
        }

        [Theory]
        [MemberData(nameof(ValidCollectionSizes))]
        public void HashSet_Generic_Constructor_int_IEqualityComparer_AddUpToAndBeyondCapacity(int capacity)
        {
            IEqualityComparer<T> comparer = GetIEqualityComparer();
            HashSet<T> set = new HashSet<T>(capacity, comparer);

            AddToCollection(set, capacity);
            Assert.Equal(capacity, set.Count);

            AddToCollection(set, capacity + 1);
            Assert.Equal(capacity + 1, set.Count);
        }

        [Fact]
        public void HashSet_Generic_Constructor_int_IEqualityComparer_Negative_ThrowsArgumentOutOfRangeException()
        {
            IEqualityComparer<T> comparer = GetIEqualityComparer();
            AssertExtensions.Throws<ArgumentOutOfRangeException>("capacity", () => new HashSet<T>(-1, comparer));
            AssertExtensions.Throws<ArgumentOutOfRangeException>("capacity", () => new HashSet<T>(int.MinValue, comparer));
        }

        #region TryGetValue

        [Fact]
        public void HashSet_Generic_TryGetValue_Contains()
        {
            T value = CreateT(1);
            HashSet<T> set = new HashSet<T> { value };
            T equalValue = CreateT(1);
            T actualValue;
            Assert.True(set.TryGetValue(equalValue, out actualValue));
            Assert.Equal(value, actualValue);
            if (!typeof(T).IsValueType)
            {
#pragma warning disable xUnit2005 // Do not use Assert.Same() on value type 'T'. Value types do not have identity. Use Assert.Equal instead.
                Assert.Same((object)value, (object)actualValue);
#pragma warning restore xUnit2005
            }
        }

        [Fact]
        public void HashSet_Generic_TryGetValue_Contains_OverwriteOutputParam()
        {
            T value = CreateT(1);
            HashSet<T> set = new HashSet<T> { value };
            T equalValue = CreateT(1);
            T actualValue = CreateT(2);
            Assert.True(set.TryGetValue(equalValue, out actualValue));
            Assert.Equal(value, actualValue);
            if (!typeof(T).IsValueType)
            {
#pragma warning disable xUnit2005 // Do not use Assert.Same() on value type 'T'. Value types do not have identity. Use Assert.Equal instead.
                Assert.Same((object)value, (object)actualValue);
#pragma warning restore xUnit2005
            }
        }

        [Fact]
        public void HashSet_Generic_TryGetValue_NotContains()
        {
            T value = CreateT(1);
            HashSet<T> set = new HashSet<T> { value };
            T equalValue = CreateT(2);
            T actualValue;
            Assert.False(set.TryGetValue(equalValue, out actualValue));
            Assert.Equal(default(T), actualValue);
        }

        [Fact]
        public void HashSet_Generic_TryGetValue_NotContains_OverwriteOutputParam()
        {
            T value = CreateT(1);
            HashSet<T> set = new HashSet<T> { value };
            T equalValue = CreateT(2);
            T actualValue = equalValue;
            Assert.False(set.TryGetValue(equalValue, out actualValue));
            Assert.Equal(default(T), actualValue);
        }

        #endregion

        #region EnsureCapacity

        [Theory]
        [MemberData(nameof(ValidCollectionSizes))]
        public void EnsureCapacity_Generic_RequestingLargerCapacity_DoesNotInvalidateEnumeration(int setLength)
        {
            HashSet<T> set = (HashSet<T>)(GenericISetFactory(setLength));
            var capacity = set.EnsureCapacity(0);
            IEnumerator valuesEnum = set.GetEnumerator();
            IEnumerator valuesListEnum = new List<T>(set).GetEnumerator();

            set.EnsureCapacity(capacity + 1); // Verify EnsureCapacity does not invalidate enumeration

            while (valuesEnum.MoveNext())
            {
                valuesListEnum.MoveNext();
                Assert.Equal(valuesListEnum.Current, valuesEnum.Current);
            }
        }

        [Fact]
        public void EnsureCapacity_Generic_NegativeCapacityRequested_Throws()
        {
            var set = new HashSet<T>();
            AssertExtensions.Throws<ArgumentOutOfRangeException>("capacity", () => set.EnsureCapacity(-1));
        }

        [Fact]
        public void EnsureCapacity_Generic_HashsetNotInitialized_RequestedZero_ReturnsZero()
        {
            var set = new HashSet<T>();
            Assert.Equal(0, set.EnsureCapacity(0));
        }

        [Theory]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        public void EnsureCapacity_Generic_HashsetNotInitialized_RequestedNonZero_CapacityIsSetToAtLeastTheRequested(int requestedCapacity)
        {
            var set = new HashSet<T>();
            Assert.InRange(set.EnsureCapacity(requestedCapacity), requestedCapacity, int.MaxValue);
        }

        [Theory]
        [InlineData(3)]
        [InlineData(7)]
        public void EnsureCapacity_Generic_RequestedCapacitySmallerThanCurrent_CapacityUnchanged(int currentCapacity)
        {
            HashSet<T> set;

            // assert capacity remains the same when ensuring a capacity smaller or equal than existing
            for (int i = 0; i <= currentCapacity; i++)
            {
                set = new HashSet<T>(currentCapacity);
                Assert.Equal(currentCapacity, set.EnsureCapacity(i));
            }
        }

        [Theory]
        [InlineData(7)]
        [InlineData(89)]
        public void EnsureCapacity_Generic_ExistingCapacityRequested_SameValueReturned(int capacity)
        {
            var set = new HashSet<T>(capacity);
            Assert.Equal(capacity, set.EnsureCapacity(capacity));

            set = (HashSet<T>)GenericISetFactory(capacity);
            Assert.Equal(capacity, set.EnsureCapacity(capacity));
        }

        [Theory]
        [InlineData(0)]
        [InlineData(1)]
        [InlineData(2)]
        [InlineData(3)]
        [InlineData(4)]
        public void EnsureCapacity_Generic_EnsureCapacityCalledTwice_ReturnsSameValue(int setLength)
        {
            HashSet<T> set = (HashSet<T>)GenericISetFactory(setLength);
            int capacity = set.EnsureCapacity(0);
            Assert.Equal(capacity, set.EnsureCapacity(0));

            set = (HashSet<T>)GenericISetFactory(setLength);
            capacity = set.EnsureCapacity(setLength);
            Assert.Equal(capacity, set.EnsureCapacity(setLength));

            set = (HashSet<T>)GenericISetFactory(setLength);
            capacity = set.EnsureCapacity(setLength + 1);
            Assert.Equal(capacity, set.EnsureCapacity(setLength + 1));
        }

        [Theory]
        [InlineData(1)]
        [InlineData(5)]
        [InlineData(7)]
        [InlineData(8)]
        public void EnsureCapacity_Generic_HashsetNotEmpty_RequestedSmallerThanCount_ReturnsAtLeastSizeOfCount(int setLength)
        {
            HashSet<T> set = (HashSet<T>)GenericISetFactory(setLength);
            Assert.InRange(set.EnsureCapacity(setLength - 1), setLength, int.MaxValue);
        }

        [Theory]
        [InlineData(7)]
        [InlineData(20)]
        public void EnsureCapacity_Generic_HashsetNotEmpty_SetsToAtLeastTheRequested(int setLength)
        {
            HashSet<T> set = (HashSet<T>)GenericISetFactory(setLength);

            // get current capacity
            int currentCapacity = set.EnsureCapacity(0);

            // assert we can update to a larger capacity
            int newCapacity = set.EnsureCapacity(currentCapacity * 2);
            Assert.InRange(newCapacity, currentCapacity * 2, int.MaxValue);
        }

        [Fact]
        public void EnsureCapacity_Generic_CapacityIsSetToPrimeNumberLargerOrEqualToRequested()
        {
            var set = new HashSet<T>();
            Assert.Equal(17, set.EnsureCapacity(17));

            set = new HashSet<T>();
            Assert.Equal(17, set.EnsureCapacity(15));

            set = new HashSet<T>();
            Assert.Equal(17, set.EnsureCapacity(13));
        }

        [Theory]
        [InlineData(2)]
        [InlineData(10)]
        public void EnsureCapacity_Generic_GrowCapacityWithFreeList(int setLength)
        {
            HashSet<T> set = (HashSet<T>)GenericISetFactory(setLength);

            // Remove the first element to ensure we have a free list.
            Assert.True(set.Remove(set.ElementAt(0)));

            int currentCapacity = set.EnsureCapacity(0);
            Assert.True(currentCapacity > 0);

            int newCapacity = set.EnsureCapacity(currentCapacity + 1);
            Assert.True(newCapacity > currentCapacity);
        }

        #endregion

        #region Remove

        [Theory]
        [MemberData(nameof(ValidPositiveCollectionSizes))]
        public void Remove_NonDefaultComparer_ComparerUsed(int capacity)
        {
            var c = new TrackingEqualityComparer<T>();
            var set = new HashSet<T>(capacity, c);

            AddToCollection(set, capacity);
            T first = set.First();
            c.EqualsCalls = 0;
            c.GetHashCodeCalls = 0;

            Assert.Equal(capacity, set.Count);
            set.Remove(first);
            Assert.Equal(capacity - 1, set.Count);

            Assert.InRange(c.EqualsCalls, 1, int.MaxValue);
            Assert.InRange(c.GetHashCodeCalls, 1, int.MaxValue);
        }

        #endregion

        #region Serialization

        [ConditionalFact(typeof(PlatformDetection), nameof(PlatformDetection.IsBinaryFormatterSupported))]
        public void ComparerSerialization()
        {
            // Strings switch between randomized and non-randomized comparers,
            // however this should never be observable externally.
            TestComparerSerialization(EqualityComparer<string>.Default);

            // OrdinalCaseSensitiveComparer is internal and (de)serializes as OrdinalComparer
            TestComparerSerialization(StringComparer.Ordinal, "System.OrdinalComparer");

            // OrdinalIgnoreCaseComparer is internal and (de)serializes as OrdinalComparer
            TestComparerSerialization(StringComparer.OrdinalIgnoreCase, "System.OrdinalComparer");
            TestComparerSerialization(StringComparer.CurrentCulture);
            TestComparerSerialization(StringComparer.CurrentCultureIgnoreCase);
            TestComparerSerialization(StringComparer.InvariantCulture);
            TestComparerSerialization(StringComparer.InvariantCultureIgnoreCase);

            // Check other types while here, IEquatable valuetype, nullable valuetype, and non IEquatable object
            TestComparerSerialization(EqualityComparer<int>.Default);
            TestComparerSerialization(EqualityComparer<int?>.Default);
            TestComparerSerialization(EqualityComparer<object>.Default);

            static void TestComparerSerialization<TCompared>(IEqualityComparer<TCompared> equalityComparer, string internalTypeName = null)
            {
                var bf = new BinaryFormatter() // Vulnerabile a deserializzazione insicura;
                var s = new MemoryStream();

                var set = new HashSet<TCompared>(equalityComparer);

                Assert.Same(equalityComparer, set.Comparer);

                bf.Serialize(s, set);
                s.Position = 0;
                set = (HashSet<TCompared>)bf.Deserialize(s);

                if (equalityComparer.Equals(EqualityComparer<string>.Default))
                {
                    // EqualityComparer<string>.Default is mapped to StringEqualityComparer, but serialized as GenericEqualityComparer<string>
                    Assert.Equal("System.Collections.Generic.GenericEqualityComparer`1[System.String]", set.Comparer.GetType().ToString());
                    return;
                }

                if (internalTypeName == null)
                {
                    Assert.IsType(equalityComparer.GetType(), set.Comparer);
                }
                else
                {
                    Assert.Equal(internalTypeName, set.Comparer.GetType().ToString());
                }

                Assert.True(equalityComparer.Equals(set.Comparer));
            }
        }

        #endregion
    }
}
