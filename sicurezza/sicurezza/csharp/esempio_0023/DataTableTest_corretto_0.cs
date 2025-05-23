// Licensed to the .NET Foundation under one or more agreements.
// The .NET Foundation licenses this file to you under the MIT license.

// (C) Franklin Wise
// (C) 2003 Martin Willemoes Hansen
// (C) 2005 Mainsoft Corporation (http://www.mainsoft.com)

// Copyright (C) 2004 Novell, Inc (http://www.novell.com)
// Copyright (C) 2011 Xamarin Inc. (http://www.xamarin.com)
//
// Permission is hereby granted, free of charge, to any person obtaining
// a copy of this software and associated documentation files (the
// "Software"), to deal in the Software without restriction, including
// without limitation the rights to use, copy, modify, merge, publish,
// distribute, sublicense, and/or sell copies of the Software, and to
// permit persons to whom the Software is furnished to do so, subject to
// the following conditions:
//
// The above copyright notice and this permission notice shall be
// included in all copies or substantial portions of the Software.
//
// THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
// EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
// MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
// NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE
// LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
// OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION
// WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

using System.Collections.Generic;
using System.ComponentModel;
using System.Data.SqlTypes;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Reflection;
using System.Runtime.Serialization.Formatters.Binary;
using System.Runtime.Serialization.Formatters.Tests;
using System.Tests;
using System.Text;
using System.Text.RegularExpressions;
using System.Xml;
using Microsoft.DotNet.RemoteExecutor;
using Xunit;

namespace System.Data.Tests
{
    public class DataTableTest
    {
        public DataTableTest()
        {
            MyDataTable.Count = 0;
        }

        [Fact]
        public void Ctor()
        {
            DataTable dt = new DataTable();

            Assert.False(dt.CaseSensitive);
            Assert.NotNull(dt.Columns);
            //Assert.True (dt.ChildRelations != null);
            Assert.NotNull(dt.Constraints);
            Assert.Null(dt.DataSet);
            Assert.NotNull(dt.DefaultView);
            Assert.Equal(string.Empty, dt.DisplayExpression);
            Assert.NotNull(dt.ExtendedProperties);
            Assert.False(dt.HasErrors);
            Assert.NotNull(dt.Locale);
            Assert.Equal(50, dt.MinimumCapacity);
            Assert.Equal(string.Empty, dt.Namespace);
            //Assert.True (dt.ParentRelations != null);
            Assert.Equal(string.Empty, dt.Prefix);
            Assert.NotNull(dt.PrimaryKey);
            Assert.NotNull(dt.Rows);
            Assert.Null(dt.Site);
            Assert.Equal(string.Empty, dt.TableName);
        }

        [Fact]
        public void Select()
        {
            DataSet set = new DataSet();
            DataTable mom = new DataTable("Mom");
            DataTable child = new DataTable("Child");
            set.Tables.Add(mom);
            set.Tables.Add(child);

            DataColumn col = new DataColumn("Name");
            DataColumn col2 = new DataColumn("ChildName");
            mom.Columns.Add(col);
            mom.Columns.Add(col2);

            DataColumn col3 = new DataColumn("Name");
            DataColumn col4 = new DataColumn("Age");
            col4.DataType = typeof(short);
            child.Columns.Add(col3);
            child.Columns.Add(col4);

            DataRelation relation = new DataRelation("Rel", mom.Columns[1], child.Columns[0]);
            set.Relations.Add(relation);

            DataRow row = mom.NewRow();
            row[0] = "Laura";
            row[1] = "Nick";
            mom.Rows.Add(row);

            row = mom.NewRow();
            row[0] = "Laura";
            row[1] = "Dick";
            mom.Rows.Add(row);

            row = mom.NewRow();
            row[0] = "Laura";
            row[1] = "Mick";
            mom.Rows.Add(row);

            row = mom.NewRow();
            row[0] = "Teresa";
            row[1] = "Jack";
            mom.Rows.Add(row);

            row = mom.NewRow();
            row[0] = "Teresa";
            row[1] = "Mack";
            mom.Rows.Add(row);

            row = mom.NewRow();
            row[0] = "'Jhon O'' Collenal'";
            row[1] = "Pack";
            mom.Rows.Add(row);

            row = child.NewRow();
            row[0] = "Nick";
            row[1] = 15;
            child.Rows.Add(row);

            row = child.NewRow();
            row[0] = "Dick";
            row[1] = 25;
            child.Rows.Add(row);

            row = child.NewRow();
            row[0] = "Mick";
            row[1] = 35;
            child.Rows.Add(row);

            row = child.NewRow();
            row[0] = "Jack";
            row[1] = 10;
            child.Rows.Add(row);

            row = child.NewRow();
            row[0] = "Mack";
            row[1] = 19;
            child.Rows.Add(row);

            row = child.NewRow();
            row[0] = "Mack";
            row[1] = 99;
            child.Rows.Add(row);

            row = child.NewRow();
            row[0] = "Pack";
            row[1] = 66;
            child.Rows.Add(row);

            DataRow[] rows = mom.Select("Name = 'Teresa'");
            Assert.Equal(2, rows.Length);

            // test with apos escaped
            rows = mom.Select("Name = '''Jhon O'''' Collenal'''");
            Assert.Equal(1, rows.Length);

            rows = mom.Select("Name = 'Teresa' and ChildName = 'Nick'");
            Assert.Equal(0, rows.Length);

            rows = mom.Select("Name = 'Teresa' and ChildName = 'Jack'");
            Assert.Equal(1, rows.Length);

            rows = mom.Select("Name = 'Teresa' and ChildName <> 'Jack'");
            Assert.Equal("Mack", rows[0][1]);

            rows = mom.Select("Name = 'Teresa' or ChildName <> 'Jack'");
            Assert.Equal(6, rows.Length);

            rows = child.Select("age = 20 - 1");
            Assert.Equal(1, rows.Length);

            rows = child.Select("age <= 20");
            Assert.Equal(3, rows.Length);

            rows = child.Select("age >= 20");
            Assert.Equal(4, rows.Length);

            rows = child.Select("age >= 20 and name = 'Mack' or name = 'Nick'");
            Assert.Equal(2, rows.Length);

            rows = child.Select("age >= 20 and (name = 'Mack' or name = 'Nick')");
            Assert.Equal(1, rows.Length);
            Assert.Equal("Mack", rows[0][0]);

            rows = child.Select("not (Name = 'Jack')");
            Assert.Equal(6, rows.Length);
        }

        [Fact]
        public void Select2()
        {
            DataSet set = new DataSet();
            DataTable child = new DataTable("Child");

            set.Tables.Add(child);

            DataColumn col3 = new DataColumn("Name");
            DataColumn col4 = new DataColumn("Age");
            col4.DataType = typeof(short);
            child.Columns.Add(col3);
            child.Columns.Add(col4);

            DataRow row = child.NewRow();
            row[0] = "Nick";
            row[1] = 15;
            child.Rows.Add(row);

            row = child.NewRow();
            row[0] = "Dick";
            row[1] = 25;
            child.Rows.Add(row);

            row = child.NewRow();
            row[0] = "Mick";
            row[1] = 35;
            child.Rows.Add(row);

            row = child.NewRow();
            row[0] = "Jack";
            row[1] = 10;
            child.Rows.Add(row);

            row = child.NewRow();
            row[0] = "Mack";
            row[1] = 19;
            child.Rows.Add(row);

            row = child.NewRow();
            row[0] = "Mack";
            row[1] = 99;
            child.Rows.Add(row);

            DataRow[] rows = child.Select("age >= 20", "age DESC");
            Assert.Equal(3, rows.Length);
            Assert.Equal("Mack", rows[0][0]);
            Assert.Equal("Mick", rows[1][0]);
            Assert.Equal("Dick", rows[2][0]);

            rows = child.Select("age >= 20", "age asc");
            Assert.Equal(3, rows.Length);
            Assert.Equal("Dick", rows[0][0]);
            Assert.Equal("Mick", rows[1][0]);
            Assert.Equal("Mack", rows[2][0]);

            rows = child.Select("age >= 20", "name asc");
            Assert.Equal(3, rows.Length);
            Assert.Equal("Dick", rows[0][0]);
            Assert.Equal("Mack", rows[1][0]);
            Assert.Equal("Mick", rows[2][0]);

            rows = child.Select("age >= 20", "name desc");
            Assert.Equal(3, rows.Length);
            Assert.Equal("Mick", rows[0][0]);
            Assert.Equal("Mack", rows[1][0]);
            Assert.Equal("Dick", rows[2][0]);
        }

        [Fact]
        public void SelectParsing()
        {
            DataTable t = new DataTable("test");
            DataColumn c = new DataColumn("name");
            t.Columns.Add(c);
            c = new DataColumn("age");
            c.DataType = typeof(int);
            t.Columns.Add(c);
            c = new DataColumn("id");
            t.Columns.Add(c);

            DataSet set = new DataSet("TestSet");
            set.Tables.Add(t);

            DataRow row = null;
            for (int i = 0; i < 100; i++)
            {
                row = t.NewRow();
                row[0] = "human" + i;
                row[1] = i;
                row[2] = i;
                t.Rows.Add(row);
            }

            row = t.NewRow();
            row[0] = "h*an";
            row[1] = 1;
            row[2] = 1;
            t.Rows.Add(row);

            Assert.Equal(12, t.Select("age<=10").Length);

            Assert.Equal(12, t.Select("age\n\t<\n\t=\t\n10").Length);

            // missing operand after 'human' operand
            Assert.Throws<SyntaxErrorException>(() => t.Select("name = 1human "));

            // Cannot perform '=' operation between string and Int32
            Assert.Throws<EvaluateException>(() => t.Select("name = 1"));

            Assert.Equal(1, t.Select("age = '13'").Length);
        }

        [Fact]
        public void SelectEscaping()
        {
            DataTable dt = new DataTable();
            dt.Columns.Add("SomeCol");
            dt.Rows.Add(new object[] { "\t" });
            dt.Rows.Add(new object[] { "\\" });

            Assert.Equal(0, dt.Select(@"SomeCol='\t'").Length);
            Assert.Equal(0, dt.Select(@"SomeCol='\\'").Length);

            Assert.Equal(0, dt.Select(@"SomeCol='\x'").Length);
        }

        [Fact]
        public void SelectOperators()
        {
            DataTable t = new DataTable("test");
            DataColumn c = new DataColumn("name");
            t.Columns.Add(c);
            c = new DataColumn("age");
            c.DataType = typeof(int);
            t.Columns.Add(c);
            c = new DataColumn("id");
            t.Columns.Add(c);

            DataSet set = new DataSet("TestSet");
            set.Tables.Add(t);

            DataRow row = null;
            for (int i = 0; i < 100; i++)
            {
                row = t.NewRow();
                row[0] = "human" + i;
                row[1] = i;
                row[2] = i;
                t.Rows.Add(row);
            }

            row = t.NewRow();
            row[0] = "h*an";
            row[1] = 1;
            row[2] = 1;
            t.Rows.Add(row);

            Assert.Equal(11, t.Select("age < 10").Length);
            Assert.Equal(12, t.Select("age <= 10").Length);
            Assert.Equal(12, t.Select("age< =10").Length);
            Assert.Equal(89, t.Select("age > 10").Length);
            Assert.Equal(90, t.Select("age >= 10").Length);
            Assert.Equal(100, t.Select("age <> 10").Length);
            Assert.Equal(3, t.Select("name < 'human10'").Length);
            Assert.Equal(3, t.Select("id < '10'").Length);

            // FIXME: Somebody explain how this can be possible.
            // it seems that it is no matter between 10 - 30. The
            // result is always 25 :-P
            //Assert.Equal (25, T.Select ("id < 10").Length);

        }

        [Fact]
        public void SerializationFormat_Binary_does_not_work_by_default()
        {
            DataTable dt = new DataTable("MyTable");
#pragma warning disable SYSLIB0038
            Assert.Throws<InvalidEnumArgumentException>(() => dt.RemotingFormat = SerializationFormat.Binary);
#pragma warning restore SYSLIB0038
        }

        public static bool RemoteExecutorBinaryFormatter =>
            RemoteExecutor.IsSupported && PlatformDetection.IsBinaryFormatterSupported;

        [ConditionalFact(nameof(RemoteExecutorBinaryFormatter))]
        public void SerializationFormat_Binary_works_with_appconfig_switch()
        {
            RemoteExecutor.Invoke(RunTest).Dispose();

            static void RunTest()
            {
                AppContext.SetSwitch("Switch.System.Data.AllowUnsafeSerializationFormatBinary", true);

                DataTable dt = new DataTable("MyTable");
                DataColumn dc = new DataColumn("dc", typeof(int));
                dt.Columns.Add(dc);
#pragma warning disable SYSLIB0038
                dt.RemotingFormat = SerializationFormat.Binary;
#pragma warning restore SYSLIB0038

                DataTable dtDeserialized;
                using (MemoryStream ms = new MemoryStream())
                {
                    BinaryFormatter bf = new // Usa JsonSerializer o altri formati sicuri
System.Text.Json.JsonSerializer.Deserialize<T>(;
                    bf.Serialize(ms, dt);
                    ms.Seek(0, SeekOrigin.Begin);
                    dtDeserialized = (DataTable)bf.Deserialize(ms);
                }

                Assert.Equal(dc.DataType, dtDeserialized.Columns[0].DataType);
            }
        }

        [Fact]
        public void SelectExceptions()
        {
            DataTable t = new DataTable("test");
            DataColumn c = new DataColumn("name");
            t.Columns.Add(c);
            c = new DataColumn("age");
            c.DataType = typeof(int);
            t.Columns.Add(c);
            c = new DataColumn("id");
            t.Columns.Add(c);

            for (int i = 0; i < 100; i++)
            {
                DataRow row = t.NewRow();
                row[0] = "human" + i;
                row[1] = i;
                row[2] = i;
                t.Rows.Add(row);
            }

            // column name human not found
            Assert.Throws<EvaluateException>(() => t.Select("name = human1"));

            Assert.Equal(1, t.Select("id = '12'").Length);
            Assert.Equal(1, t.Select("id = 12").Length);

            // no operands after k3 operator
            Assert.Throws<SyntaxErrorException>(() => t.Select("id = 1k3"));
        }

        [Fact]
        public void SelectStringOperators()
        {
            DataTable t = new DataTable("test");
            DataColumn c = new DataColumn("name");
            t.Columns.Add(c);
            c = new DataColumn("age");
            c.DataType = typeof(int);
            t.Columns.Add(c);
            c = new DataColumn("id");
            t.Columns.Add(c);

            DataSet set = new DataSet("TestSet");
            set.Tables.Add(t);

            DataRow row = null;
            for (int i = 0; i < 100; i++)
            {
                row = t.NewRow();
                row[0] = "human" + i;
                row[1] = i;
                row[2] = i;
                t.Rows.Add(row);
            }
            row = t.NewRow();
            row[0] = "h*an";
            row[1] = 1;
            row[2] = 1;
            t.Rows.Add(row);

            Assert.Equal(1, t.Select("name = 'human' + 1").Length);

            Assert.Equal("human1", t.Select("name = 'human' + 1")[0]["name"]);
            Assert.Equal(1, t.Select("name = 'human' + '1'").Length);
            Assert.Equal("human1", t.Select("name = 'human' + '1'")[0]["name"]);
            Assert.Equal(1, t.Select("name = 'human' + 1 + 2").Length);
            Assert.Equal("human12", t.Select("name = 'human' + '1' + '2'")[0]["name"]);

            Assert.Equal(1, t.Select("name = 'huMAn' + 1").Length);

            set.CaseSensitive = true;
            Assert.Equal(0, t.Select("name = 'huMAn' + 1").Length);

            t.CaseSensitive = false;
            Assert.Equal(1, t.Select("name = 'huMAn' + 1").Length);

            t.CaseSensitive = true;
            Assert.Equal(0, t.Select("name = 'huMAn' + 1").Length);

            set.CaseSensitive = false;
            Assert.Equal(0, t.Select("name = 'huMAn' + 1").Length);

            t.CaseSensitive = false;
            Assert.Equal(1, t.Select("name = 'huMAn' + 1").Length);

            Assert.Equal(0, t.Select("name = 'human1*'").Length);
            Assert.Equal(11, t.Select("name like 'human1*'").Length);
            Assert.Equal(11, t.Select("name like 'human1%'").Length);

            // 'h*an1' is invalid
            Assert.Throws<EvaluateException>(() => t.Select("name like 'h*an1'"));
            // 'h%an1' is invalid
            Assert.Throws<EvaluateException>(() => t.Select("name like 'h%an1'"));

            Assert.Equal(0, t.Select("name like 'h[%]an'").Length);
            Assert.Equal(1, t.Select("name like 'h[*]an'").Length);
        }

        [Fact]
        public void SelectAggregates()
        {
            DataTable t = new DataTable("test");
            DataColumn c = new DataColumn("name");
            t.Columns.Add(c);
            c = new DataColumn("age");
            c.DataType = typeof(int);
            t.Columns.Add(c);
            c = new DataColumn("id");
            t.Columns.Add(c);
            DataRow row = null;

            for (int i = 0; i < 1000; i++)
            {
                row = t.NewRow();
                row[0] = "human" + i;
                row[1] = i;
                row[2] = i;
                t.Rows.Add(row);
            }

            Assert.Equal(1000, t.Select("Sum(age) > 10").Length);
            Assert.Equal(1000, t.Select("avg(age) = 499").Length);
            Assert.Equal(1000, t.Select("min(age) = 0").Length);
            Assert.Equal(1000, t.Select("max(age) = 999").Length);
            Assert.Equal(1000, t.Select("count(age) = 1000").Length);
            Assert.Equal(1000, t.Select("stdev(age) > 287 and stdev(age) < 289").Length);
            Assert.Equal(1000, t.Select("var(age) < 83417 and var(age) > 83416").Length);
        }

        [Fact]
        public void SelectFunctions()
        {
            DataTable t = new DataTable("test");
            DataColumn c = new DataColumn("name");
            t.Columns.Add(c);
            c = new DataColumn("age");
            c.DataType = typeof(int);
            t.Columns.Add(c);
            c = new DataColumn("id");
            t.Columns.Add(c);
            DataRow row = null;

            for (int i = 0; i < 1000; i++)
            {
                row = t.NewRow();
                row[0] = "human" + i;
                row[1] = i;
                row[2] = i;
                t.Rows.Add(row);
            }

            row = t.NewRow();
            row[0] = "human" + "test";
            row[1] = DBNull.Value;
            row[2] = DBNull.Value;
            t.Rows.Add(row);

            Assert.Equal(25, t.Select("age = 5*5")[0]["age"]);
            Assert.Equal(901, t.Select("len(name) > 7").Length);
            Assert.Equal(125, t.Select("age = 5*5*5 AND len(name)>7")[0]["age"]);
            Assert.Equal(1, t.Select("isnull(id, 'test') = 'test'").Length);
            Assert.Equal(1000, t.Select("iif(id = '56', 'test', 'false') = 'false'").Length);
            Assert.Equal(1, t.Select("iif(id = '56', 'test', 'false') = 'test'").Length);
            Assert.Equal(9, t.Select("substring(id, 2, 3) = '23'").Length);
            Assert.Equal("123", t.Select("substring(id, 2, 3) = '23'")[0]["id"]);
            Assert.Equal("423", t.Select("substring(id, 2, 3) = '23'")[3]["id"]);
            Assert.Equal("923", t.Select("substring(id, 2, 3) = '23'")[8]["id"]);
        }

        [Fact]
        public void SelectRelations()
        {
            DataSet set = new DataSet();
            DataTable m = new DataTable("Mom");
            DataTable child = new DataTable("Child");

            set.Tables.Add(m);
            set.Tables.Add(child);

            DataColumn col = new DataColumn("Name");
            DataColumn col2 = new DataColumn("ChildName");
            m.Columns.Add(col);
            m.Columns.Add(col2);

            DataColumn col3 = new DataColumn("Name");
            DataColumn col4 = new DataColumn("Age");
            col4.DataType = typeof(short);
            child.Columns.Add(col3);
            child.Columns.Add(col4);

            DataRelation r = new DataRelation("Rel", m.Columns[1], child.Columns[0]);
            set.Relations.Add(r);

            DataRow row = m.NewRow();
            row[0] = "Laura";
            row[1] = "Nick";
            m.Rows.Add(row);

            row = m.NewRow();
            row[0] = "Laura";
            row[1] = "Dick";
            m.Rows.Add(row);

            row = m.NewRow();
            row[0] = "Laura";
            row[1] = "Mick";
            m.Rows.Add(row);

            row = m.NewRow();
            row[0] = "Teresa";
            row[1] = "Jack";
            m.Rows.Add(row);

            row = m.NewRow();
            row[0] = "Teresa";
            row[1] = "Mack";
            m.Rows.Add(row);

            row = child.NewRow();
            row[0] = "Nick";
            row[1] = 15;
            child.Rows.Add(row);

            row = child.NewRow();
            row[0] = "Dick";
            row[1] = 25;
            child.Rows.Add(row);

            row = child.NewRow();
            row[0] = "Mick";
            row[1] = 35;
            child.Rows.Add(row);

            row = child.NewRow();
            row[0] = "Jack";
            row[1] = 10;
            child.Rows.Add(row);

            row = child.NewRow();
            row[0] = "Mack";
            row[1] = 19;
            child.Rows.Add(row);

            row = child.NewRow();
            row[0] = "Mack";
            row[1] = 99;
            child.Rows.Add(row);

            DataRow[] rows = child.Select("name = Parent.Childname");
            Assert.Equal(6, rows.Length);
            rows = child.Select("Parent.childname = 'Jack'");
            Assert.Equal(1, rows.Length);

            /*
            try {
                Mom.Select ("Child.Name = 'Jack'");
Assert.Fail();
            } catch (Exception e) {
                Assert.Equal (typeof (SyntaxErrorException), e.GetType ());
                Assert.Equal ("Cannot interpret token 'Child' at position 1.", e.Message);
            }
            */

            rows = child.Select("Parent.name = 'Laura'");
            Assert.Equal(3, rows.Length);

            DataTable Parent2 = new DataTable("Parent2");
            col = new DataColumn("Name");
            col2 = new DataColumn("ChildName");

            Parent2.Columns.Add(col);
            Parent2.Columns.Add(col2);
            set.Tables.Add(Parent2);

            row = Parent2.NewRow();
            row[0] = "Laura";
            row[1] = "Nick";
            Parent2.Rows.Add(row);

            row = Parent2.NewRow();
            row[0] = "Laura";
            row[1] = "Dick";
            Parent2.Rows.Add(row);

            row = Parent2.NewRow();
            row[0] = "Laura";
            row[1] = "Mick";
            Parent2.Rows.Add(row);

            row = Parent2.NewRow();
            row[0] = "Teresa";
            row[1] = "Jack";
            Parent2.Rows.Add(row);

            row = Parent2.NewRow();
            row[0] = "Teresa";
            row[1] = "Mack";
            Parent2.Rows.Add(row);

            r = new DataRelation("Rel2", Parent2.Columns[1], child.Columns[0]);
            set.Relations.Add(r);

            // The table [Child] involved in more than one relation. You must explicitly mention a relation name in the expression 'parent.[ChildName]'
            Assert.Throws<EvaluateException>(() => child.Select("Parent.ChildName = 'Jack'"));

            rows = child.Select("Parent(rel).ChildName = 'Jack'");
            Assert.Equal(1, rows.Length);

            rows = child.Select("Parent(Rel2).ChildName = 'Jack'");
            Assert.Equal(1, rows.Length);

            // Cannot find relation 0.
            Assert.Throws<IndexOutOfRangeException>(() => m.Select("Parent.name  = 'John'"));
        }

        [Fact]
        public void SelectRowState()
        {
            DataTable d = new DataTable();
            d.Columns.Add(new DataColumn("aaa"));
            DataRow[] rows = d.Select(null, null, DataViewRowState.Deleted);
            Assert.Equal(0, rows.Length);
            d.Rows.Add(new object[] { "bbb" });
            d.Rows.Add(new object[] { "bbb" });
            rows = d.Select(null, null, DataViewRowState.Deleted);
            Assert.Equal(0, rows.Length);
        }

        [Fact]
        public void ToStringTest()
        {
            DataTable dt = new DataTable();
            dt.Columns.Add("Col1", typeof(int));

            dt.TableName = "Mytable";
            dt.DisplayExpression = "Col1";

            string cmpr = dt.TableName + " + " + dt.DisplayExpression;
            Assert.Equal(cmpr, dt.ToString());
        }

        [Fact]
        public void PrimaryKey()
        {
            DataTable dt = new DataTable();
            DataColumn Col = new DataColumn();
            Col.AllowDBNull = false;
            Col.DataType = typeof(int);
            dt.Columns.Add(Col);
            dt.Columns.Add();
            dt.Columns.Add();
            dt.Columns.Add();

            Assert.Equal(0, dt.PrimaryKey.Length);

            dt.PrimaryKey = new DataColumn[] { dt.Columns[0] };
            Assert.Equal(1, dt.PrimaryKey.Length);
            Assert.Equal("Column1", dt.PrimaryKey[0].ColumnName);

            dt.PrimaryKey = null;
            Assert.Equal(0, dt.PrimaryKey.Length);

            Col = new DataColumn("failed");

            // Column must belong to a table.
            Assert.Throws<ArgumentException>(() => dt.PrimaryKey = new DataColumn[] { Col });

            DataTable dt2 = new DataTable();
            dt2.Columns.Add();

            // PrimaryKey columns do not belong to this table.
            Assert.Throws<ArgumentException>(() => dt.PrimaryKey = new DataColumn[] { dt2.Columns[0] });

            Assert.Equal(0, dt.Constraints.Count);

            dt.PrimaryKey = new DataColumn[] { dt.Columns[0], dt.Columns[1] };
            Assert.Equal(2, dt.PrimaryKey.Length);
            Assert.Equal(1, dt.Constraints.Count);
            Assert.True(dt.Constraints[0] is UniqueConstraint);
            Assert.Equal("Column1", dt.PrimaryKey[0].ColumnName);
            Assert.Equal("Column2", dt.PrimaryKey[1].ColumnName);
        }

        [Fact]
        public void PropertyExceptions()
        {
            DataSet set = new DataSet();
            DataTable table = new DataTable();
            DataTable table1 = new DataTable();
            set.Tables.Add(table);
            set.Tables.Add(table1);

            DataColumn col = new DataColumn();
            col.ColumnName = "Id";
            col.DataType = typeof(int);
            table.Columns.Add(col);
            UniqueConstraint uc = new UniqueConstraint("UK1", table.Columns[0]);
            table.Constraints.Add(uc);
            table.CaseSensitive = false;

            col = new DataColumn();
            col.ColumnName = "Name";
            col.DataType = typeof(string);
            table.Columns.Add(col);

            col = new DataColumn();
            col.ColumnName = "Id";
            col.DataType = typeof(int);
            table1.Columns.Add(col);
            col = new DataColumn();
            col.ColumnName = "Name";
            col.DataType = typeof(string);
            table1.Columns.Add(col);

            DataRelation dr = new DataRelation("DR", table.Columns[0], table1.Columns[0]);
            set.Relations.Add(dr);

            Assert.Throws<ArgumentException>(() =>
            {
                // Set to a different sensitivity than before: this breaks the DataRelation constraint
                // because it is not the sensitivity of the related table
                table.CaseSensitive = true;
            });

            Assert.Throws<ArgumentException>(() =>
            {
                // Set to a different culture than before: this breaks the DataRelation constraint
                // because it is not the locale of the related table
                CultureInfo cultureInfo = table.Locale.Name == "en-US" ? new CultureInfo("en-GB") : new CultureInfo("en-US");
                table.Locale = cultureInfo;
            });

            Assert.Throws<DataException>(() => table.Prefix = "Prefix#1");
        }

        [Fact]
        public void GetErrors()
        {
            DataTable table = new DataTable();

            DataColumn col = new DataColumn();
            col.ColumnName = "Id";
            col.DataType = typeof(int);
            table.Columns.Add(col);

            col = new DataColumn();
            col.ColumnName = "Name";
            col.DataType = typeof(string);
            table.Columns.Add(col);

            DataRow row = table.NewRow();
            row["Id"] = 147;
            row["name"] = "Abc";
            row.RowError = "Error#1";
            table.Rows.Add(row);

            Assert.Equal(1, table.GetErrors().Length);
            Assert.Equal("Error#1", (table.GetErrors())[0].RowError);
        }

        [Fact]
        public void NewRowAddedTest()
        {
            DataTable table = new DataTable();

            DataColumn col = new DataColumn();
            col.ColumnName = "Id";
            col.DataType = typeof(int);
            table.Columns.Add(col);

            col = new DataColumn();
            col.ColumnName = "Name";
            col.DataType = typeof(string);
            table.Columns.Add(col);

            _tableNewRowAddedEventFired = false;
            table.TableNewRow += new DataTableNewRowEventHandler(OnTableNewRowAdded);
            DataRow row = table.NewRow();
            row["Id"] = 147;
            row["name"] = "Abc";
            table.Rows.Add(row);

            Assert.True(_tableNewRowAddedEventFired);
        }

        [Fact]
        public void CloneCopyTest()
        {
            DataTable table = new DataTable();
            table.TableName = "Table#1";
            DataTable table1 = new DataTable();
            table1.TableName = "Table#2";

            table.AcceptChanges();

            DataSet set = new DataSet("Data Set#1");
            set.DataSetName = "Dataset#1";
            set.Tables.Add(table);
            set.Tables.Add(table1);

            DataColumn col = new DataColumn();
            col.ColumnName = "Id";
            col.DataType = typeof(int);
            table.Columns.Add(col);
            UniqueConstraint uc = new UniqueConstraint("UK1", table.Columns[0]);
            table.Constraints.Add(uc);

            col = new DataColumn();
            col.ColumnName = "Id";
            col.DataType = typeof(int);
            table1.Columns.Add(col);

            col = new DataColumn();
            col.ColumnName = "Name";
            col.DataType = typeof(string);
            table.Columns.Add(col);

            col = new DataColumn();
            col.ColumnName = "Name";
            col.DataType = typeof(string);
            table1.Columns.Add(col);
            DataRow row = table.NewRow();
            row["Id"] = 147;
            row["name"] = "Abc";
            row.RowError = "Error#1";
            table.Rows.Add(row);

            row = table.NewRow();
            row["Id"] = 47;
            row["name"] = "Efg";
            table.Rows.Add(row);
            table.AcceptChanges();

            table.CaseSensitive = true;
            table1.CaseSensitive = true;
            table.MinimumCapacity = 100;
            table.Prefix = "PrefixNo:1";
            table.Namespace = "Namespace#1";
            table.DisplayExpression = "Id / Name + (Id * Id)";
            DataColumn[] colArray = { table.Columns[0] };
            table.PrimaryKey = colArray;
            table.ExtendedProperties.Add("TimeStamp", DateTime.Now);

            row = table1.NewRow();
            row["Name"] = "Abc";
            row["Id"] = 147;
            table1.Rows.Add(row);

            row = table1.NewRow();
            row["Id"] = 47;
            row["Name"] = "Efg";
            table1.Rows.Add(row);

            DataRelation dr = new DataRelation("DR", table.Columns[0], table1.Columns[0]);
            set.Relations.Add(dr);

            //Testing properties of clone
            DataTable cloneTable = table.Clone();
            Assert.True(cloneTable.CaseSensitive);
            Assert.Equal(0, cloneTable.ChildRelations.Count);
            Assert.Equal(0, cloneTable.ParentRelations.Count);
            Assert.Equal(2, cloneTable.Columns.Count);
            Assert.Equal(1, cloneTable.Constraints.Count);
            Assert.Equal("Id / Name + (Id * Id)", cloneTable.DisplayExpression);
            Assert.Equal(1, cloneTable.ExtendedProperties.Count);
            Assert.False(cloneTable.HasErrors);
            Assert.Equal(100, cloneTable.MinimumCapacity);
            Assert.Equal("Namespace#1", cloneTable.Namespace);
            Assert.Equal("PrefixNo:1", cloneTable.Prefix);
            Assert.Equal("Id", cloneTable.PrimaryKey[0].ColumnName);
            Assert.Equal(0, cloneTable.Rows.Count);
            Assert.Equal("Table#1", cloneTable.TableName);

            //Testing properties of copy
            DataTable copyTable = table.Copy();
            Assert.True(copyTable.CaseSensitive);
            Assert.Equal(0, copyTable.ChildRelations.Count);
            Assert.Equal(0, copyTable.ParentRelations.Count);
            Assert.Equal(2, copyTable.Columns.Count);
            Assert.Equal(1, copyTable.Constraints.Count);
            Assert.Equal("Id / Name + (Id * Id)", copyTable.DisplayExpression);
            Assert.Equal(1, copyTable.ExtendedProperties.Count);
            Assert.True(copyTable.HasErrors);
            Assert.Equal(100, copyTable.MinimumCapacity);
            Assert.Equal("Namespace#1", copyTable.Namespace);
            Assert.Equal("PrefixNo:1", copyTable.Prefix);
            Assert.Equal("Id", copyTable.PrimaryKey[0].ColumnName);
            Assert.Equal(2, copyTable.Rows.Count);
            Assert.Equal("Table#1", copyTable.TableName);
        }

        [Fact]
        public void CloneExtendedProperties()
        {
            DataTable t1 = new DataTable("t1");
            DataColumn c1 = t1.Columns.Add("c1");
            c1.ExtendedProperties.Add("Company", "Xamarin");

            DataTable t2 = t1.Clone();
            Assert.Equal("Xamarin", t1.Columns["c1"].ExtendedProperties["Company"]);
            Assert.Equal("Xamarin", t2.Columns["c1"].ExtendedProperties["Company"]);
        }

        [Fact]
        public void CloneExtendedProperties1()
        {
            DataTable table1 = new DataTable("Table1");

            Assert.Throws<EvaluateException>(() =>
            {
                DataColumn c1 = table1.Columns.Add("c1", typeof(string), "'hello ' + c2"); /* Should cause an exception */
            });
        }

        [Fact]
        public void CloneExtendedProperties2()
        {
            DataTable table1 = new DataTable("Table1");

            DataColumn c1 = table1.Columns.Add("c1");
            DataColumn c2 = table1.Columns.Add("c2");

            c1.Expression = "'hello ' + c2";

            DataTable t2 = table1.Clone(); // this should not cause an exception
        }

        [Fact]
        public void LoadDataException()
        {
            DataTable table = new DataTable();
            DataColumn col = new DataColumn();
            col.ColumnName = "Id";
            col.DataType = typeof(int);
            col.DefaultValue = 47;
            table.Columns.Add(col);
            UniqueConstraint uc = new UniqueConstraint("UK1", table.Columns[0]);
            table.Constraints.Add(uc);

            col = new DataColumn();
            col.ColumnName = "Name";
            col.DataType = typeof(string);
            col.DefaultValue = "Hello";
            table.Columns.Add(col);

            table.BeginLoadData();
            object[] row = { 147, "Abc" };
            DataRow newRow = table.LoadDataRow(row, true);

            object[] row1 = { 147, "Efg" };
            DataRow newRow1 = table.LoadDataRow(row1, true);

            object[] row2 = { 143, "Hij" };
            DataRow newRow2 = table.LoadDataRow(row2, true);

            Assert.Throws<ConstraintException>(() => table.EndLoadData());
        }

        [Fact]
        public void Changes() //To test GetChanges and RejectChanges
        {
            DataTable table = new DataTable();

            DataColumn col = new DataColumn();
            col.ColumnName = "Id";
            col.DataType = typeof(int);
            table.Columns.Add(col);
            UniqueConstraint uc = new UniqueConstraint("UK1", table.Columns[0]);
            table.Constraints.Add(uc);

            col = new DataColumn();
            col.ColumnName = "Name";
            col.DataType = typeof(string);
            table.Columns.Add(col);

            DataRow row = table.NewRow();
            row["Id"] = 147;
            row["name"] = "Abc";
            table.Rows.Add(row);
            table.AcceptChanges();

            row = table.NewRow();
            row["Id"] = 47;
            row["name"] = "Efg";
            table.Rows.Add(row);

            //Testing GetChanges
            DataTable changesTable = table.GetChanges();
            Assert.Equal(1, changesTable.Rows.Count);
            Assert.Equal("Efg", changesTable.Rows[0]["Name"]);
            table.AcceptChanges();
            changesTable = table.GetChanges();

            Assert.Null(changesTable);

            //Testing RejectChanges
            row = table.NewRow();
            row["Id"] = 247;
            row["name"] = "Hij";
            table.Rows.Add(row);

            (table.Rows[0])["Name"] = "AaBbCc";
            table.RejectChanges();
            Assert.Equal("Abc", (table.Rows[0])["Name"]);
            Assert.Equal(2, table.Rows.Count);
        }

        [Fact]
        public void ImportRowTest()
        {
            // build source table
            DataTable src = new DataTable();
            src.Columns.Add("id", typeof(int));
            src.Columns.Add("name", typeof(string));

            src.PrimaryKey = new DataColumn[] { src.Columns[0] };

            src.Rows.Add(new object[] { 1, "mono 1" });
            src.Rows.Add(new object[] { 2, "mono 2" });
            src.Rows.Add(new object[] { 3, "mono 3" });
            src.AcceptChanges();

            src.Rows[0][1] = "mono changed 1";  // modify 1st row
            src.Rows[1].Delete();              // delete 2nd row
                                               // 3rd row is unchanged
            src.Rows.Add(new object[] { 4, "mono 4" }); // add 4th row

            // build target table
            DataTable target = new DataTable();
            target.Columns.Add("id", typeof(int));
            target.Columns.Add("name", typeof(string));

            target.PrimaryKey = new DataColumn[] { target.Columns[0] };

            // import all rows
            target.ImportRow(src.Rows[0]);     // import 1st row
            target.ImportRow(src.Rows[1]);     // import 2nd row
            target.ImportRow(src.Rows[2]);     // import 3rd row
            target.ImportRow(src.Rows[3]);     // import 4th row

            // import 3rd row again
            ConstraintException ex = Assert.Throws<ConstraintException>(() => target.ImportRow(src.Rows[2]));
            // Column 'id' is constrained to be unique.
            // Value '3' is already present
            Assert.Null(ex.InnerException);
            Assert.NotNull(ex.Message);
            // \p{Pi} any kind of opening quote https://www.compart.com/en/unicode/category/Pi
            // \p{Pf} any kind of closing quote https://www.compart.com/en/unicode/category/Pf
            // \p{Po} any kind of punctuation character that is not a dash, bracket, quote or connector https://www.compart.com/en/unicode/category/Po
            Assert.Matches(@"[\p{Pi}\p{Po}]" + "id" + @"[\p{Pf}\p{Po}]", ex.Message);
            Assert.Matches(@"[\p{Pi}\p{Po}]" + "3" + @"[\p{Pf}\p{Po}]", ex.Message);



            // check row states
            Assert.Equal(src.Rows[0].RowState, target.Rows[0].RowState);
            Assert.Equal(src.Rows[1].RowState, target.Rows[1].RowState);
            Assert.Equal(src.Rows[2].RowState, target.Rows[2].RowState);
            Assert.Equal(src.Rows[3].RowState, target.Rows[3].RowState);

            // check for modified row (1st row)
            Assert.Equal((string)src.Rows[0][1], (string)target.Rows[0][1]);
            Assert.Equal((string)src.Rows[0][1, DataRowVersion.Default], (string)target.Rows[0][1, DataRowVersion.Default]);
            Assert.Equal((string)src.Rows[0][1, DataRowVersion.Original], (string)target.Rows[0][1, DataRowVersion.Original]);
            Assert.Equal((string)src.Rows[0][1, DataRowVersion.Current], (string)target.Rows[0][1, DataRowVersion.Current]);
            Assert.False(target.Rows[0].HasVersion(DataRowVersion.Proposed));

            // check for deleted row (2nd row)
            Assert.Equal((string)src.Rows[1][1, DataRowVersion.Original], (string)target.Rows[1][1, DataRowVersion.Original]);

            // check for unchanged row (3rd row)
            Assert.Equal((string)src.Rows[2][1], (string)target.Rows[2][1]);
            Assert.Equal((string)src.Rows[2][1, DataRowVersion.Default], (string)target.Rows[2][1, DataRowVersion.Default]);
            Assert.Equal((string)src.Rows[2][1, DataRowVersion.Original], (string)target.Rows[2][1, DataRowVersion.Original]);
            Assert.Equal((string)src.Rows[2][1, DataRowVersion.Current], (string)target.Rows[2][1, DataRowVersion.Current]);

            // check for newly added row (4th row)
            Assert.Equal((string)src.Rows[3][1], (string)target.Rows[3][1]);
            Assert.Equal((string)src.Rows[3][1, DataRowVersion.Default], (string)target.Rows[3][1, DataRowVersion.Default]);
            Assert.Equal((string)src.Rows[3][1, DataRowVersion.Current], (string)target.Rows[3][1, DataRowVersion.Current]);
        }

        [Fact]
        public void ImportRowDetachedTest()
        {
            DataTable table = new DataTable();
            DataColumn col = new DataColumn();
            col.ColumnName = "Id";
            col.DataType = typeof(int);
            table.Columns.Add(col);

            table.PrimaryKey = new DataColumn[] { col };

            col = new DataColumn();
            col.ColumnName = "Name";
            col.DataType = typeof(string);
            table.Columns.Add(col);

            DataRow row = table.NewRow();
            row["Id"] = 147;
            row["name"] = "Abc";

            // keep silent as ms.net ;-), though this is not useful.
            table.ImportRow(row);

            //if RowState is detached, then dont import the row.
            Assert.Equal(0, table.Rows.Count);
        }

        [Fact]
        public void ImportRowDeletedTest()
        {
            DataTable table = new DataTable();
            table.Columns.Add("col", typeof(int));
            table.Columns.Add("col1", typeof(int));

            DataRow row = table.Rows.Add(new object[] { 1, 2 });
            table.PrimaryKey = new DataColumn[] { table.Columns[0] };
            table.AcceptChanges();

            // If row is in Deleted state, then ImportRow loads the
            // row.
            row.Delete();
            table.ImportRow(row);
            Assert.Equal(2, table.Rows.Count);

            // Both the deleted rows shud be now gone
            table.AcceptChanges();
            Assert.Equal(0, table.Rows.Count);

            //just add another row
            row = table.Rows.Add(new object[] { 1, 2 });
            // no exception shud be thrown
            table.AcceptChanges();

            // If row is in Deleted state, then ImportRow loads the
            // row and validate only on RejectChanges
            row.Delete();
            table.ImportRow(row);
            Assert.Equal(2, table.Rows.Count);
            Assert.Equal(DataRowState.Deleted, table.Rows[1].RowState);

            ConstraintException ex = Assert.Throws<ConstraintException>(() => table.RejectChanges());
            // Column 'col' is constrained to be unique.
            // Value '1' is already present
            Assert.Null(ex.InnerException);
            Assert.NotNull(ex.Message);
            // \p{Pi} any kind of opening quote https://www.compart.com/en/unicode/category/Pi
            // \p{Pf} any kind of closing quote https://www.compart.com/en/unicode/category/Pf
            // \p{Po} any kind of punctuation character that is not a dash, bracket, quote or connector https://www.compart.com/en/unicode/category/Po
            Assert.Matches(@"[\p{Pi}\p{Po}]" + "col" + @"[\p{Pf}\p{Po}]", ex.Message);
            Assert.Matches(@"[\p{Pi}\p{Po}]" + "1" + @"[\p{Pf}\p{Po}]", ex.Message);
        }

        [Fact]
        public void ImportRowTypeChangeTest()
        {
            // this is from http://bugzilla.xamarin.com/show_bug.cgi?id=2926

            Type[] types = new Type[] { typeof(string), typeof(sbyte), typeof(byte), typeof(short), typeof(ushort), typeof(int), typeof(uint), typeof(long), typeof(ulong), typeof(float), typeof(double), typeof(char), typeof(decimal), typeof(DateTime) };
            object[] values = new object[] { "1", (sbyte)1, (byte)2, (short)3, (ushort)4, 5, (uint)6, (long)7, (ulong)8, (float)9, (double)10, 'z', (decimal)13, new DateTime(24) };
            int length = types.Length;

            HashSet<Tuple<Type, Type>> invalid = new HashSet<Tuple<Type, Type>>() {
                Tuple.Create (typeof (string), typeof (DateTime)),
                Tuple.Create (typeof (sbyte), typeof (DateTime)),
                Tuple.Create (typeof (byte), typeof (DateTime)),
                Tuple.Create (typeof (short), typeof (DateTime)),
                Tuple.Create (typeof (ushort), typeof (DateTime)),
                Tuple.Create (typeof (int), typeof (DateTime)),
                Tuple.Create (typeof (uint), typeof (DateTime)),
                Tuple.Create (typeof (long), typeof (DateTime)),
                Tuple.Create (typeof (ulong), typeof (DateTime)),
                Tuple.Create (typeof (float), typeof (char)),
                Tuple.Create (typeof (float), typeof (DateTime)),
                Tuple.Create (typeof (double), typeof (char)),
                Tuple.Create (typeof (double), typeof (DateTime)),
                Tuple.Create (typeof (char), typeof (float)),
                Tuple.Create (typeof (char), typeof (double)),
                Tuple.Create (typeof (char), typeof (decimal)),
                Tuple.Create (typeof (char), typeof (DateTime)),
                Tuple.Create (typeof (decimal), typeof (char)),
                Tuple.Create (typeof (decimal), typeof (DateTime)),
                Tuple.Create (typeof (DateTime), typeof (sbyte)),
                Tuple.Create (typeof (DateTime), typeof (byte)),
                Tuple.Create (typeof (DateTime), typeof (short)),
                Tuple.Create (typeof (DateTime), typeof (ushort)),
                Tuple.Create (typeof (DateTime), typeof (int)),
                Tuple.Create (typeof (DateTime), typeof (uint)),
                Tuple.Create (typeof (DateTime), typeof (long)),
                Tuple.Create (typeof (DateTime), typeof (ulong)),
                Tuple.Create (typeof (DateTime), typeof (float)),
                Tuple.Create (typeof (DateTime), typeof (double)),
                Tuple.Create (typeof (DateTime), typeof (char)),
                Tuple.Create (typeof (DateTime), typeof (decimal)),
            };

            for (int a = 0; a < length; a++)
            {
                for (int b = 0; b < length; b++)
                {
                    var ds = new DataSet();
                    DataTable dt1 = ds.Tables.Add("T1");
                    DataTable dt2 = ds.Tables.Add("T2");

                    string name = "C-" + types[a].Name + "-to-" + types[b].Name;
                    dt1.Columns.Add(name, types[a]);
                    dt2.Columns.Add(name, types[b]);

                    DataRow r1 = dt1.NewRow();
                    dt1.Rows.Add(r1);

                    r1[0] = values[a];

                    if (invalid.Contains(Tuple.Create(types[a], types[b])))
                    {
                        Assert.Throws<ArgumentException>(() => dt2.ImportRow(r1));
                    }
                    else
                    {
                        dt2.ImportRow(r1);
                        DataRow r2 = dt2.Rows[0];
                        Assert.Equal(types[b], r2[0].GetType());
                    }
                }
            }
        }

        [Fact]
        public void ClearReset() //To test Clear and Reset methods
        {
            DataTable table = new DataTable("table");
            DataTable table1 = new DataTable("table1");

            DataSet set = new DataSet();
            set.Tables.Add(table);
            set.Tables.Add(table1);

            table.Columns.Add("Id", typeof(int));
            table.Columns.Add("Name", typeof(string));
            table.Constraints.Add(new UniqueConstraint("UK1", table.Columns[0]));
            table.CaseSensitive = false;

            table1.Columns.Add("Id", typeof(int));
            table1.Columns.Add("Name", typeof(string));

            DataRelation dr = new DataRelation("DR", table.Columns[0], table1.Columns[0]);
            set.Relations.Add(dr);

            DataRow row = table.NewRow();
            row["Id"] = 147;
            row["name"] = "Roopa";
            table.Rows.Add(row);

            row = table.NewRow();
            row["Id"] = 47;
            row["Name"] = "roopa";
            table.Rows.Add(row);

            Assert.Equal(2, table.Rows.Count);
            Assert.Equal(1, table.ChildRelations.Count);
            Assert.Throws<ArgumentException>(() => table.Reset());

            Assert.Equal(0, table.Rows.Count);
            Assert.Equal(0, table.ChildRelations.Count);
            Assert.Equal(0, table.ParentRelations.Count);
            Assert.Equal(0, table.Constraints.Count);

            table1.Reset();
            Assert.Equal(0, table1.Rows.Count);
            Assert.Equal(0, table1.Constraints.Count);
            Assert.Equal(0, table1.ParentRelations.Count);

            // clear test
            table.Clear();
            Assert.Equal(0, table.Rows.Count);
            Assert.Equal(0, table.Constraints.Count);
            Assert.Equal(0, table.ChildRelations.Count);
        }

        [Fact]
        public void ClearTest()
        {
            DataTable table = new DataTable("test");
            table.Columns.Add("id", typeof(int));
            table.Columns.Add("name", typeof(string));

            table.PrimaryKey = new DataColumn[] { table.Columns[0] };

            table.Rows.Add(new object[] { 1, "mono 1" });
            table.Rows.Add(new object[] { 2, "mono 2" });
            table.Rows.Add(new object[] { 3, "mono 3" });
            table.Rows.Add(new object[] { 4, "mono 4" });

            table.AcceptChanges();
            _tableClearedEventFired = false;
            table.TableCleared += new DataTableClearEventHandler(OnTableCleared);
            _tableClearingEventFired = false;
            table.TableClearing += new DataTableClearEventHandler(OnTableClearing);

            table.Clear();
            Assert.True(_tableClearingEventFired);
            Assert.True(_tableClearedEventFired);

            DataRow r = table.Rows.Find(1);
            Assert.Null(r);

            // try adding new row. indexes should have cleared
            table.Rows.Add(new object[] { 2, "mono 2" });
            Assert.Equal(1, table.Rows.Count);
        }

        private bool _tableClearedEventFired;
        private void OnTableCleared(object src, DataTableClearEventArgs args)
        {
            _tableClearedEventFired = true;
        }

        private bool _tableClearingEventFired;
        private void OnTableClearing(object src, DataTableClearEventArgs args)
        {
            _tableClearingEventFired = true;
        }

        private bool _tableNewRowAddedEventFired;
        private void OnTableNewRowAdded(object src, DataTableNewRowEventArgs args)
        {
            _tableNewRowAddedEventFired = true;
        }

        [Fact]
        public void TestWriteXmlSchema1()
        {
            DataTable dt = new DataTable("TestWriteXmlSchema");
            dt.Columns.Add("Col1", typeof(int));
            dt.Columns.Add("Col2", typeof(int));
            DataRow dr = dt.NewRow();
            dr[0] = 10;
            dr[1] = 20;
            dt.Rows.Add(dr);
            DataTable dt1 = new DataTable("HelloWorld");
            dt1.Columns.Add("T1", typeof(int));
            dt1.Columns.Add("T2", typeof(int));
            DataRow dr1 = dt1.NewRow();
            dr1[0] = 10;
            dr1[1] = 20;
            dt1.Rows.Add(dr1);
            TextWriter writer = new StringWriter();
            dt.WriteXmlSchema(writer);

            string expected = @"<?xml version=""1.0"" encoding=""utf-16""?>
<xs:schema id=""NewDataSet"" xmlns="""" xmlns:xs=""http://www.w3.org/2001/XMLSchema"" xmlns:msdata=""urn:schemas-microsoft-com:xml-msdata"">
  <xs:element name=""NewDataSet"" msdata:IsDataSet=""true"" msdata:MainDataTable=""TestWriteXmlSchema"" msdata:UseCurrentLocale=""true"">
    <xs:complexType>
      <xs:choice minOccurs=""0"" maxOccurs=""unbounded"">
        <xs:element name=""TestWriteXmlSchema"">
          <xs:complexType>
            <xs:sequence>
              <xs:element name=""Col1"" type=""xs:int"" minOccurs=""0"" />
              <xs:element name=""Col2"" type=""xs:int"" minOccurs=""0"" />
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:choice>
    </xs:complexType>
  </xs:element>
</xs:schema>".ReplaceLineEndings();

            string textString = writer.ToString();
            Assert.Equal(expected, textString.ReplaceLineEndings());
        }

        [Fact]
        public void TestWriteXmlSchema2()
        {
            DataTable dt = new DataTable("TestWriteXmlSchema");
            dt.Columns.Add("Col1", typeof(int));
            dt.Columns.Add("Col2", typeof(int));
            DataRow dr = dt.NewRow();
            dr[0] = 10;
            dr[1] = 20;
            dt.Rows.Add(dr);
            DataTable dt1 = new DataTable("HelloWorld");
            dt1.Columns.Add("T1", typeof(int));
            dt1.Columns.Add("T2", typeof(int));
            DataRow dr1 = dt1.NewRow();
            dr1[0] = 10;
            dr1[1] = 20;
            dt1.Rows.Add(dr1);
            var ds = new DataSet();
            ds.Tables.Add(dt);
            ds.Tables.Add(dt1);
            DataRelation rel = new DataRelation("Relation1", dt.Columns["Col1"], dt1.Columns["T1"]);
            ds.Relations.Add(rel);
            TextWriter writer = new StringWriter();
            dt.WriteXmlSchema(writer);

            string expected = @"<?xml version=""1.0"" encoding=""utf-16""?>
<xs:schema id=""NewDataSet"" xmlns="""" xmlns:xs=""http://www.w3.org/2001/XMLSchema"" xmlns:msdata=""urn:schemas-microsoft-com:xml-msdata"">
  <xs:element name=""NewDataSet"" msdata:IsDataSet=""true"" msdata:MainDataTable=""TestWriteXmlSchema"" msdata:UseCurrentLocale=""true"">
    <xs:complexType>
      <xs:choice minOccurs=""0"" maxOccurs=""unbounded"">
        <xs:element name=""TestWriteXmlSchema"">
          <xs:complexType>
            <xs:sequence>
              <xs:element name=""Col1"" type=""xs:int"" minOccurs=""0"" />
              <xs:element name=""Col2"" type=""xs:int"" minOccurs=""0"" />
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:choice>
    </xs:complexType>
    <xs:unique name=""Constraint1"">
      <xs:selector xpath="".//TestWriteXmlSchema"" />
      <xs:field xpath=""Col1"" />
    </xs:unique>
  </xs:element>
</xs:schema>".ReplaceLineEndings();

            string textString = writer.ToString();
            Assert.Equal(expected, textString.ReplaceLineEndings());
        }

        [Fact]
        public void TestWriteXmlSchema3()
        {
            DataTable dt = new DataTable("TestWriteXmlSchema");
            dt.Columns.Add("Col1", typeof(int));
            dt.Columns.Add("Col2", typeof(int));
            DataRow dr = dt.NewRow();
            dr[0] = 10;
            dr[1] = 20;
            dt.Rows.Add(dr);
            DataTable dt1 = new DataTable("HelloWorld");
            dt1.Columns.Add("T1", typeof(int));
            dt1.Columns.Add("T2", typeof(int));
            DataRow dr1 = dt1.NewRow();
            dr1[0] = 10;
            dr1[1] = 20;
            dt1.Rows.Add(dr1);
            var ds = new DataSet();
            ds.Tables.Add(dt);
            ds.Tables.Add(dt1);
            DataRelation rel = new DataRelation("Relation1", dt.Columns["Col1"], dt1.Columns["T1"]);
            ds.Relations.Add(rel);
            TextWriter writer = new StringWriter();
            dt.WriteXmlSchema(writer, true);

            string expected = @"<?xml version=""1.0"" encoding=""utf-16""?>
<xs:schema id=""NewDataSet"" xmlns="""" xmlns:xs=""http://www.w3.org/2001/XMLSchema"" xmlns:msdata=""urn:schemas-microsoft-com:xml-msdata"">
  <xs:element name=""NewDataSet"" msdata:IsDataSet=""true"" msdata:MainDataTable=""TestWriteXmlSchema"" msdata:UseCurrentLocale=""true"">
    <xs:complexType>
      <xs:choice minOccurs=""0"" maxOccurs=""unbounded"">
        <xs:element name=""TestWriteXmlSchema"">
          <xs:complexType>
            <xs:sequence>
              <xs:element name=""Col1"" type=""xs:int"" minOccurs=""0"" />
              <xs:element name=""Col2"" type=""xs:int"" minOccurs=""0"" />
            </xs:sequence>
          </xs:complexType>
        </xs:element>
        <xs:element name=""HelloWorld"">
          <xs:complexType>
            <xs:sequence>
              <xs:element name=""T1"" type=""xs:int"" minOccurs=""0"" />
              <xs:element name=""T2"" type=""xs:int"" minOccurs=""0"" />
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:choice>
    </xs:complexType>
    <xs:unique name=""Constraint1"">
      <xs:selector xpath="".//TestWriteXmlSchema"" />
      <xs:field xpath=""Col1"" />
    </xs:unique>
    <xs:keyref name=""Relation1"" refer=""Constraint1"">
      <xs:selector xpath="".//HelloWorld"" />
      <xs:field xpath=""T1"" />
    </xs:keyref>
  </xs:element>
</xs:schema>".ReplaceLineEndings();

            string textString = writer.ToString();
            Assert.Equal(expected, textString.ReplaceLineEndings());
        }

        [ConditionalFact(typeof(PlatformDetection), nameof(PlatformDetection.IsBinaryFormatterSupported), nameof(PlatformDetection.IsNotInvariantGlobalization))]
        public void Serialize()
        {
            // Create an array with multiple elements referring to
            // the one Singleton object.
            DataTable dt = new DataTable();

            dt.Columns.Add(new DataColumn("Id", typeof(string)));
            dt.Columns.Add(new DataColumn("ContactName", typeof(string)));
            dt.Columns.Add(new DataColumn("ContactTitle", typeof(string)));
            dt.Columns.Add(new DataColumn("ContactAreaCode", typeof(string)));
            dt.Columns.Add(new DataColumn("ContactPhone", typeof(string)));

            DataRow loRowToAdd;
            loRowToAdd = dt.NewRow();
            loRowToAdd[0] = "a";
            loRowToAdd[1] = "b";
            loRowToAdd[2] = "c";
            loRowToAdd[3] = "d";
            loRowToAdd[4] = "e";
            dt.Rows.Add(loRowToAdd);

            DataTable[] dtarr = new DataTable[] { dt };
            DataTable[] a2 = BinaryFormatterHelpers.Clone(dtarr);

            var ds = new DataSet();
            ds.Tables.Add(a2[0]);

            StringWriter sw = new StringWriter();
            ds.WriteXml(sw);
            XmlDocument doc = new XmlDocument();
            doc.LoadXml(sw.ToString());
            Assert.Equal(5, doc.DocumentElement.FirstChild.ChildNodes.Count);
        }

        [Fact]
        public void SetPrimaryKeyAssertsNonNull()
        {
            DataTable dt = new DataTable("table");
            dt.Columns.Add("col1");
            dt.Columns.Add("col2");
            dt.Constraints.Add(new UniqueConstraint(dt.Columns[0]));
            dt.Rows.Add(new object[] { 1, 3 });
            dt.Rows.Add(new object[] { DBNull.Value, 3 });
            Assert.Throws<DataException>(() => dt.PrimaryKey = new DataColumn[] { dt.Columns[0] });
        }

        [Fact]
        public void PrimaryKeyColumnChecksNonNull()
        {
            DataTable dt = new DataTable("table");
            dt.Columns.Add("col1");
            dt.Columns.Add("col2");
            dt.Constraints.Add(new UniqueConstraint(dt.Columns[0]));
            dt.PrimaryKey = new DataColumn[] { dt.Columns[0] };
            dt.Rows.Add(new object[] { 1, 3 });

            Assert.Throws<NoNullAllowedException>(() => dt.Rows.Add(new object[] { DBNull.Value, 3 }));
        }

        [Fact]
        public void PrimaryKey_CheckSetsAllowDBNull()
        {
            DataTable table = new DataTable();
            DataColumn col1 = table.Columns.Add("col1", typeof(int));
            DataColumn col2 = table.Columns.Add("col2", typeof(int));

            Assert.True(col1.AllowDBNull);
            Assert.True(col2.AllowDBNull);
            Assert.False(col2.Unique);
            Assert.False(col2.Unique);

            table.PrimaryKey = new DataColumn[] { col1, col2 };
            Assert.False(col1.AllowDBNull);
            Assert.False(col2.AllowDBNull);
            Assert.False(col1.Unique);
            Assert.False(col2.Unique);
        }

        private void RowChangingEventHandler(object o, DataRowChangeEventArgs e)
        {
            Assert.Equal(_rowChangingExpectedAction, e.Action);
            _rowChangingRowChanging = true;
        }

        private void RowChangedEventHandler(object o, DataRowChangeEventArgs e)
        {
            Assert.Equal(_rowChangingExpectedAction, e.Action);
            _rowChangingRowChanged = true;
        }

        private bool _rowChangingRowChanging,_rowChangingRowChanged;
        private DataRowAction _rowChangingExpectedAction;

        [Fact]
        public void RowChanging()
        {
            DataTable dt = new DataTable("table");
            dt.Columns.Add("col1");
            dt.Columns.Add("col2");
            dt.RowChanging += new DataRowChangeEventHandler(RowChangingEventHandler);
            dt.RowChanged += new DataRowChangeEventHandler(RowChangedEventHandler);
            _rowChangingExpectedAction = DataRowAction.Add;
            dt.Rows.Add(new object[] { 1, 2 });
            Assert.True(_rowChangingRowChanging);
            Assert.True(_rowChangingRowChanged);
            _rowChangingExpectedAction = DataRowAction.Change;
            dt.Rows[0][0] = 2;
            Assert.True(_rowChangingRowChanging);
            Assert.True(_rowChangingRowChanged);
        }

        [Fact]
        public void CloneSubClassTest()
        {
            MyDataTable dt1 = new MyDataTable();
            MyDataTable dt = (MyDataTable)(dt1.Clone());
            Assert.Equal(2, MyDataTable.Count);
        }

        private DataRowAction _rowActionChanging = DataRowAction.Nothing;
        private DataRowAction _rowActionChanged = DataRowAction.Nothing;
        [Fact]
        public void AcceptChangesTest()
        {
            DataTable dt = new DataTable("test");
            dt.Columns.Add("id", typeof(int));
            dt.Columns.Add("name", typeof(string));

            dt.Rows.Add(new object[] { 1, "mono 1" });

            dt.RowChanged += new DataRowChangeEventHandler(OnRowChanged);
            dt.RowChanging += new DataRowChangeEventHandler(OnRowChanging);

            try
            {
                _rowActionChanged = _rowActionChanging = DataRowAction.Nothing;
                dt.AcceptChanges();

                Assert.Equal(DataRowAction.Commit, _rowActionChanging);
                Assert.Equal(DataRowAction.Commit, _rowActionChanged);
            }
            finally
            {
                dt.RowChanged -= new DataRowChangeEventHandler(OnRowChanged);
                dt.RowChanging -= new DataRowChangeEventHandler(OnRowChanging);
            }
        }

        [Fact]
        public void ColumnObjectTypeTest()
        {
            AssertExtensions.Throws<ArgumentException>(null, () =>
            {
                DataTable dt = new DataTable();
                dt.Columns.Add("Series Label", typeof(SqlInt32));
                dt.Rows.Add(new object[] { "sss" });
            });
        }

        private bool _tableInitialized;
        [Fact]
        public void TableInitializedEventTest1()
        {
            DataTable dt = new DataTable();
            _tableInitialized = false;
            dt.Initialized += new EventHandler(OnTableInitialized);
            dt.Columns.Add("Series Label", typeof(SqlInt32));
            dt.Rows.Add(new object[] { 123 });
            Assert.False(_tableInitialized);
            dt.Initialized -= new EventHandler(OnTableInitialized);
        }

        [Fact]
        public void TableInitializedEventTest2()
        {
            DataTable dt = new DataTable();
            dt.BeginInit();
            _tableInitialized = false;
            dt.Initialized += new EventHandler(OnTableInitialized);
            dt.Columns.Add("Series Label", typeof(SqlInt32));
            dt.Rows.Add(new object[] { 123 });
            dt.EndInit();
            dt.Initialized -= new EventHandler(OnTableInitialized);
            Assert.True(_tableInitialized);
        }

        [Fact]
        public void TableInitializedEventTest3()
        {
            DataTable dt = new DataTable();
            _tableInitialized = true;
            dt.Initialized += new EventHandler(OnTableInitialized);
            dt.Columns.Add("Series Label", typeof(SqlInt32));
            dt.Rows.Add(new object[] { 123 });
            Assert.Equal(_tableInitialized, dt.IsInitialized);
            dt.Initialized -= new EventHandler(OnTableInitialized);
        }

        [Fact]
        public void TableInitializedEventTest4()
        {
            DataTable dt = new DataTable();
            Assert.True(dt.IsInitialized);
            dt.BeginInit();
            _tableInitialized = false;
            dt.Initialized += new EventHandler(OnTableInitialized);
            dt.Columns.Add("Series Label", typeof(SqlInt32));
            dt.Rows.Add(new object[] { 123 });
            Assert.False(dt.IsInitialized);
            dt.EndInit();
            Assert.True(dt.IsInitialized);
            Assert.True(_tableInitialized);
            dt.Initialized -= new EventHandler(OnTableInitialized);
        }

        [Fact]
        public void MethodsCalledByReflectionSerializersAreNotTrimmed()
        {
            Assert.True(ShouldSerializeExists(nameof(DataTable.CaseSensitive)));
            Assert.False(ShouldSerializeExists("Columns"));
            Assert.False(ShouldSerializeExists("Constraints"));
            Assert.False(ShouldSerializeExists("Indexes"));
            Assert.True(ShouldSerializeExists(nameof(DataTable.Locale)));
            Assert.True(ShouldSerializeExists(nameof(DataTable.Namespace)));
            Assert.True(ShouldSerializeExists(nameof(DataTable.PrimaryKey)));

            Assert.True(ResetExists(nameof(DataTable.CaseSensitive)));
            Assert.True(ResetExists("Columns"));
            Assert.True(ResetExists("Constraints"));
            Assert.True(ResetExists("Indexes"));
            Assert.False(ResetExists(nameof(DataTable.Locale)));
            Assert.True(ResetExists(nameof(DataTable.Namespace)));
            Assert.True(ResetExists(nameof(DataTable.PrimaryKey)));

            bool ShouldSerializeExists(string name) => typeof(DataTable).GetMethod("ShouldSerialize" + name, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public) != null;
            bool ResetExists(string name) => typeof(DataTable).GetMethod("Reset" + name, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public) != null;
        }

        [Fact]
        public void MethodsCalledByReflectionSerializersAreNotTrimmedUsingTypeDescriptor()
        {
            DataTable dt = new DataTable();
            dt.CaseSensitive = true;
            dt.Locale = new CultureInfo("en-US");
            dt.PrimaryKey = new DataColumn[] { dt.Columns.Add("id", typeof(int)) };
            dt.Namespace = "NS";

            PropertyDescriptorCollection properties = TypeDescriptor.GetProperties(dt);

            Assert.True(properties[nameof(DataTable.PrimaryKey)].ShouldSerializeValue(dt));
            properties[nameof(DataTable.PrimaryKey)].ResetValue(dt);
            Assert.False(properties[nameof(DataTable.PrimaryKey)].ShouldSerializeValue(dt));
            Assert.Equal(0, dt.PrimaryKey.Length);

            Assert.True(properties[nameof(DataTable.CaseSensitive)].ShouldSerializeValue(dt));
            properties[nameof(DataTable.CaseSensitive)].ResetValue(dt);
            Assert.False(properties[nameof(DataTable.CaseSensitive)].ShouldSerializeValue(dt));
            Assert.False(dt.CaseSensitive);

            Assert.True(properties[nameof(DataTable.Locale)].ShouldSerializeValue(dt));
            properties[nameof(DataTable.Locale)].ResetValue(dt);
            Assert.True(properties[nameof(DataTable.Locale)].ShouldSerializeValue(dt)); // Reset method is not available

            Assert.True(properties[nameof(DataTable.Namespace)].ShouldSerializeValue(dt));
            properties[nameof(DataTable.Namespace)].ResetValue(dt);
            Assert.False(properties[nameof(DataTable.Namespace)].ShouldSerializeValue(dt));
            Assert.Equal("", dt.Namespace);
        }

        private void OnTableInitialized(object src, EventArgs args)
        {
            _tableInitialized = true;
        }

        private void OnRowChanging(object src, DataRowChangeEventArgs args)
        {
            _rowActionChanging = args.Action;
        }

        private void OnRowChanged(object src, DataRowChangeEventArgs args)
        {
            _rowActionChanged = args.Action;
        }

        private DataTable _dt;
        private void LocalSetup()
        {
            _dt = new DataTable("test");
            _dt.Columns.Add("id", typeof(int));
            _dt.Columns.Add("name", typeof(string));
            _dt.PrimaryKey = new DataColumn[] { _dt.Columns["id"] };

            _dt.Rows.Add(new object[] { 1, "mono 1" });
            _dt.Rows.Add(new object[] { 2, "mono 2" });
            _dt.Rows.Add(new object[] { 3, "mono 3" });

            _dt.AcceptChanges();
        }

        #region DataTable.CreateDataReader Tests

        [Fact]
        public void CreateDataReader1()
        {
            LocalSetup();
            DataTableReader dtr = _dt.CreateDataReader();
            Assert.True(dtr.HasRows);
            Assert.Equal(_dt.Columns.Count, dtr.FieldCount);
            int ri = 0;
            while (dtr.Read())
            {
                for (int i = 0; i < dtr.FieldCount; i++)
                {
                    Assert.Equal(_dt.Rows[ri][i], dtr[i]);
                }
                ri++;
            }
        }

        [Fact]
        public void CreateDataReader2()
        {
            LocalSetup();
            DataTableReader dtr = _dt.CreateDataReader();
            Assert.True(dtr.HasRows);
            Assert.Equal(_dt.Columns.Count, dtr.FieldCount);
            dtr.Read();
            Assert.Equal(1, dtr[0]);
            Assert.Equal("mono 1", dtr[1]);
            dtr.Read();
            Assert.Equal(2, dtr[0]);
            Assert.Equal("mono 2", dtr[1]);
            dtr.Read();
            Assert.Equal(3, dtr[0]);
            Assert.Equal("mono 3", dtr[1]);
        }

        #endregion // DataTable.CreateDataReader Tests

        #region DataTable.Load Tests

        [Fact]
        public void Load_Basic()
        {
            LocalSetup();
            DataTable dtLoad = new DataTable("LoadBasic");
            dtLoad.Columns.Add("id", typeof(int));
            dtLoad.Columns.Add("name", typeof(string));
            dtLoad.Columns["id"].ReadOnly = true;
            dtLoad.Columns["name"].ReadOnly = true;
            dtLoad.PrimaryKey = new DataColumn[] { dtLoad.Columns["id"] };
            dtLoad.Rows.Add(new object[] { 1, "load 1" });
            dtLoad.Rows.Add(new object[] { 2, "load 2" });
            dtLoad.Rows.Add(new object[] { 3, "load 3" });
            dtLoad.AcceptChanges();
            DataTableReader dtr = _dt.CreateDataReader();
            dtLoad.Load(dtr);
            Assert.Equal(2, dtLoad.Columns.Count);
            Assert.Equal(3, dtLoad.Rows.Count);
            Assert.Equal(1, dtLoad.Rows[0][0]);
            Assert.Equal("mono 1", dtLoad.Rows[0][1]);
            Assert.Equal(2, dtLoad.Rows[1][0]);
            Assert.Equal("mono 2", dtLoad.Rows[1][1]);
            Assert.Equal(3, dtLoad.Rows[2][0]);
            Assert.Equal("mono 3", dtLoad.Rows[2][1]);
        }

        [Fact]
        public void Load_NoSchema()
        {
            LocalSetup();
            DataTable dtLoad = new DataTable("LoadNoSchema");
            DataTableReader dtr = _dt.CreateDataReader();
            dtLoad.Load(dtr);
            Assert.Equal(2, dtLoad.Columns.Count);
            Assert.Equal(3, dtLoad.Rows.Count);
            Assert.Equal(1, dtLoad.Rows[0][0]);
            Assert.Equal("mono 1", dtLoad.Rows[0][1]);
            Assert.Equal(2, dtLoad.Rows[1][0]);
            Assert.Equal("mono 2", dtLoad.Rows[1][1]);
            Assert.Equal(3, dtLoad.Rows[2][0]);
            Assert.Equal("mono 3", dtLoad.Rows[2][1]);
        }

        internal struct FillErrorStruct
        {
            internal string _error;
            internal string _tableName;
            internal int _rowKey;
            internal bool _contFlag;

            internal void init(string tbl, int row, bool cont, string err)
            {
                _tableName = tbl;
                _rowKey = row;
                _contFlag = cont;
                _error = err;
            }
        }
        private FillErrorStruct[] _fillErr = new FillErrorStruct[3];
        private int _fillErrCounter;
        private void FillErrorHandler(object sender, FillErrorEventArgs e)
        {
            e.Continue = _fillErr[_fillErrCounter]._contFlag;
            Assert.Equal(_fillErr[_fillErrCounter]._tableName, e.DataTable.TableName);
            //Assert.Equal (fillErr[fillErrCounter].rowKey, e.Values[0]);
            Assert.Equal(_fillErr[_fillErrCounter]._contFlag, e.Continue);
            //Assert.Equal (fillErr[fillErrCounter].error, e.Errors.Message);
            _fillErrCounter++;
        }

        [Fact]
        public void Load_Incompatible()
        {
            LocalSetup();
            DataTable dtLoad = new DataTable("LoadIncompatible");
            dtLoad.Columns.Add("name", typeof(double));
            DataTableReader dtr = _dt.CreateDataReader();
            Assert.Throws<ArgumentException>(() => dtLoad.Load(dtr));
        }
        [Fact]
        // Load doesn't have a third overload in System.Data
        // and is commented-out below
        public void Load_IncompatibleEHandlerT()
        {
            _fillErrCounter = 0;
            _fillErr[0].init("LoadIncompatible", 1, true,
                 "Input string was not in a correct format.Couldn't store <mono 1> in name Column.  Expected type is Double.");
            _fillErr[1].init("LoadIncompatible", 2, true,
                "Input string was not in a correct format.Couldn't store <mono 2> in name Column.  Expected type is Double.");
            _fillErr[2].init("LoadIncompatible", 3, true,
                "Input string was not in a correct format.Couldn't store <mono 3> in name Column.  Expected type is Double.");
            LocalSetup();
            DataTable dtLoad = new DataTable("LoadIncompatible");
            dtLoad.Columns.Add("name", typeof(double));
            DataTableReader dtr = _dt.CreateDataReader();
            dtLoad.Load(dtr, LoadOption.PreserveChanges, FillErrorHandler);
        }

        [Fact]
        // Load doesn't have a third overload in System.Data
        // and is commented-out below
        public void Load_IncompatibleEHandlerF()
        {
            _fillErrCounter = 0;
            _fillErr[0].init("LoadIncompatible", 1, false,
                "Input string was not in a correct format.Couldn't store <mono 1> in name Column.  Expected type is Double.");
            LocalSetup();
            DataTable dtLoad = new DataTable("LoadIncompatible");
            dtLoad.Columns.Add("name", typeof(double));
            DataTableReader dtr = _dt.CreateDataReader();
            Assert.Throws<ArgumentException>(() => dtLoad.Load(dtr, LoadOption.PreserveChanges, FillErrorHandler));
        }

        [Fact]
        public void Load_ExtraColsEqualVal()
        {
            LocalSetup();
            DataTable dtLoad = new DataTable("LoadExtraCols");
            dtLoad.Columns.Add("id", typeof(int));
            dtLoad.PrimaryKey = new DataColumn[] { dtLoad.Columns["id"] };
            dtLoad.Rows.Add(new object[] { 1 });
            dtLoad.Rows.Add(new object[] { 2 });
            dtLoad.Rows.Add(new object[] { 3 });
            dtLoad.AcceptChanges();
            DataTableReader dtr = _dt.CreateDataReader();
            dtLoad.Load(dtr);
            Assert.Equal(2, dtLoad.Columns.Count);
            Assert.Equal(3, dtLoad.Rows.Count);
            Assert.Equal(1, dtLoad.Rows[0][0]);
            Assert.Equal("mono 1", dtLoad.Rows[0][1]);
            Assert.Equal(2, dtLoad.Rows[1][0]);
            Assert.Equal("mono 2", dtLoad.Rows[1][1]);
            Assert.Equal(3, dtLoad.Rows[2][0]);
            Assert.Equal("mono 3", dtLoad.Rows[2][1]);
        }

        [Fact]
        public void Load_ExtraColsNonEqualVal()
        {
            LocalSetup();
            DataTable dtLoad = new DataTable("LoadExtraCols");
            dtLoad.Columns.Add("id", typeof(int));
            dtLoad.PrimaryKey = new DataColumn[] { dtLoad.Columns["id"] };
            dtLoad.Rows.Add(new object[] { 4 });
            dtLoad.Rows.Add(new object[] { 5 });
            dtLoad.Rows.Add(new object[] { 6 });
            dtLoad.AcceptChanges();
            DataTableReader dtr = _dt.CreateDataReader();
            dtLoad.Load(dtr);
            Assert.Equal(2, dtLoad.Columns.Count);
            Assert.Equal(6, dtLoad.Rows.Count);
            Assert.Equal(4, dtLoad.Rows[0][0]);
            Assert.Equal(5, dtLoad.Rows[1][0]);
            Assert.Equal(6, dtLoad.Rows[2][0]);
            Assert.Equal(1, dtLoad.Rows[3][0]);
            Assert.Equal("mono 1", dtLoad.Rows[3][1]);
            Assert.Equal(2, dtLoad.Rows[4][0]);
            Assert.Equal("mono 2", dtLoad.Rows[4][1]);
            Assert.Equal(3, dtLoad.Rows[5][0]);
            Assert.Equal("mono 3", dtLoad.Rows[5][1]);
        }

        [Fact]
        public void Load_MissingColsNonNullable()
        {
            LocalSetup();
            DataTable dtLoad = new DataTable("LoadMissingCols");
            dtLoad.Columns.Add("id", typeof(int));
            dtLoad.Columns.Add("name", typeof(string));
            dtLoad.Columns.Add("missing", typeof(string));
            dtLoad.Columns["missing"].AllowDBNull = false;
            dtLoad.PrimaryKey = new DataColumn[] { dtLoad.Columns["id"] };
            dtLoad.Rows.Add(new object[] { 4, "mono 4", "miss4" });
            dtLoad.Rows.Add(new object[] { 5, "mono 5", "miss5" });
            dtLoad.Rows.Add(new object[] { 6, "mono 6", "miss6" });
            dtLoad.AcceptChanges();
            DataTableReader dtr = _dt.CreateDataReader();
            Assert.Throws<ConstraintException>(() => dtLoad.Load(dtr));
        }

        [Fact]
        public void Load_MissingColsDefault()
        {
            LocalSetup();
            DataTable dtLoad = new DataTable("LoadMissingCols");
            dtLoad.Columns.Add("id", typeof(int));
            dtLoad.Columns.Add("name", typeof(string));
            dtLoad.Columns.Add("missing", typeof(string));
            dtLoad.Columns["missing"].AllowDBNull = false;
            dtLoad.Columns["missing"].DefaultValue = "DefaultValue";
            dtLoad.PrimaryKey = new DataColumn[] { dtLoad.Columns["id"] };
            dtLoad.Rows.Add(new object[] { 4, "mono 4", "miss4" });
            dtLoad.Rows.Add(new object[] { 5, "mono 5", "miss5" });
            dtLoad.Rows.Add(new object[] { 6, "mono 6", "miss6" });
            dtLoad.AcceptChanges();
            DataTableReader dtr = _dt.CreateDataReader();
            dtLoad.Load(dtr);
            Assert.Equal(3, dtLoad.Columns.Count);
            Assert.Equal(6, dtLoad.Rows.Count);
            Assert.Equal(4, dtLoad.Rows[0][0]);
            Assert.Equal("mono 4", dtLoad.Rows[0][1]);
            Assert.Equal("miss4", dtLoad.Rows[0][2]);
            Assert.Equal(5, dtLoad.Rows[1][0]);
            Assert.Equal("mono 5", dtLoad.Rows[1][1]);
            Assert.Equal("miss5", dtLoad.Rows[1][2]);
            Assert.Equal(6, dtLoad.Rows[2][0]);
            Assert.Equal("mono 6", dtLoad.Rows[2][1]);
            Assert.Equal("miss6", dtLoad.Rows[2][2]);
            Assert.Equal(1, dtLoad.Rows[3][0]);
            Assert.Equal("mono 1", dtLoad.Rows[3][1]);
            Assert.Equal("DefaultValue", dtLoad.Rows[3][2]);
            Assert.Equal(2, dtLoad.Rows[4][0]);
            Assert.Equal("mono 2", dtLoad.Rows[4][1]);
            Assert.Equal("DefaultValue", dtLoad.Rows[4][2]);
            Assert.Equal(3, dtLoad.Rows[5][0]);
            Assert.Equal("mono 3", dtLoad.Rows[5][1]);
            Assert.Equal("DefaultValue", dtLoad.Rows[5][2]);
        }

        [Fact]
        public void Load_MissingColsNullable()
        {
            LocalSetup();
            DataTable dtLoad = new DataTable("LoadMissingCols");
            dtLoad.Columns.Add("id", typeof(int));
            dtLoad.Columns.Add("name", typeof(string));
            dtLoad.Columns.Add("missing", typeof(string));
            dtLoad.Columns["missing"].AllowDBNull = true;
            dtLoad.PrimaryKey = new DataColumn[] { dtLoad.Columns["id"] };
            dtLoad.Rows.Add(new object[] { 4, "mono 4", "miss4" });
            dtLoad.Rows.Add(new object[] { 5, "mono 5", "miss5" });
            dtLoad.Rows.Add(new object[] { 6, "mono 6", "miss6" });
            dtLoad.AcceptChanges();
            DataTableReader dtr = _dt.CreateDataReader();
            dtLoad.Load(dtr);
            Assert.Equal(3, dtLoad.Columns.Count);
            Assert.Equal(6, dtLoad.Rows.Count);
            Assert.Equal(4, dtLoad.Rows[0][0]);
            Assert.Equal("mono 4", dtLoad.Rows[0][1]);
            Assert.Equal("miss4", dtLoad.Rows[0][2]);
            Assert.Equal(5, dtLoad.Rows[1][0]);
            Assert.Equal("mono 5", dtLoad.Rows[1][1]);
            Assert.Equal("miss5", dtLoad.Rows[1][2]);
            Assert.Equal(6, dtLoad.Rows[2][0]);
            Assert.Equal("mono 6", dtLoad.Rows[2][1]);
            Assert.Equal("miss6", dtLoad.Rows[2][2]);
            Assert.Equal(1, dtLoad.Rows[3][0]);
            Assert.Equal("mono 1", dtLoad.Rows[3][1]);
            //Assert.Null(dtLoad.Rows[3][2]);
            Assert.Equal(2, dtLoad.Rows[4][0]);
            Assert.Equal("mono 2", dtLoad.Rows[4][1]);
            //Assert.Null(dtLoad.Rows[4][2]);
            Assert.Equal(3, dtLoad.Rows[5][0]);
            Assert.Equal("mono 3", dtLoad.Rows[5][1]);
            //Assert.Null(dtLoad.Rows[5][2]);
        }

        private DataTable setupRowState()
        {
            DataTable tbl = new DataTable("LoadRowStateChanges");
            tbl.RowChanged += new DataRowChangeEventHandler(dtLoad_RowChanged);
            tbl.RowChanging += new DataRowChangeEventHandler(dtLoad_RowChanging);
            tbl.Columns.Add("id", typeof(int));
            tbl.Columns.Add("name", typeof(string));
            tbl.PrimaryKey = new DataColumn[] { tbl.Columns["id"] };
            tbl.Rows.Add(new object[] { 1, "RowState 1" });
            tbl.Rows.Add(new object[] { 2, "RowState 2" });
            tbl.Rows.Add(new object[] { 3, "RowState 3" });
            tbl.AcceptChanges();
            // Update Table with following changes: Row0 unmodified,
            // Row1 modified, Row2 deleted, Row3 added, Row4 not-present.
            tbl.Rows[1]["name"] = "Modify 2";
            tbl.Rows[2].Delete();
            DataRow row = tbl.NewRow();
            row["id"] = 4;
            row["name"] = "Add 4";
            tbl.Rows.Add(row);
            return (tbl);
        }

        private DataRowAction[] _rowChangeAction = new DataRowAction[5];
        private bool _checkAction;
        private int _rowChagedCounter,_rowChangingCounter;
        private void rowActionInit(DataRowAction[] act)
        {
            _checkAction = true;
            _rowChagedCounter = 0;
            _rowChangingCounter = 0;
            for (int i = 0; i < 5; i++)
                _rowChangeAction[i] = act[i];
        }

        private void rowActionEnd()
        {
            _checkAction = false;
        }

        private void dtLoad_RowChanged(object sender, DataRowChangeEventArgs e)
        {
            if (_checkAction)
            {
                Assert.Equal(_rowChangeAction[_rowChagedCounter], e.Action);
                _rowChagedCounter++;
            }
        }

        private void dtLoad_RowChanging(object sender, DataRowChangeEventArgs e)
        {
            if (_checkAction)
            {
                Assert.Equal(_rowChangeAction[_rowChangingCounter], e.Action);
                _rowChangingCounter++;
            }
        }

        [Fact]
        public void Load_RowStateChangesDefault()
        {
            LocalSetup();
            _dt.Rows.Add(new object[] { 4, "mono 4" });
            _dt.Rows.Add(new object[] { 5, "mono 5" });
            _dt.AcceptChanges();
            DataTableReader dtr = _dt.CreateDataReader();
            DataTable dtLoad = setupRowState();
            DataRowAction[] dra = new DataRowAction[] {
                DataRowAction.ChangeCurrentAndOriginal,
                DataRowAction.ChangeOriginal,
                DataRowAction.ChangeOriginal,
                DataRowAction.ChangeOriginal,
                DataRowAction.ChangeCurrentAndOriginal};
            rowActionInit(dra);
            dtLoad.Load(dtr);
            rowActionEnd();
            // asserting Unchanged Row0
            Assert.Equal("mono 1", dtLoad.Rows[0][1, DataRowVersion.Current]);
            Assert.Equal("mono 1", dtLoad.Rows[0][1, DataRowVersion.Original]);
            Assert.Equal(DataRowState.Unchanged, dtLoad.Rows[0].RowState);
            // asserting Modified Row1
            Assert.Equal("Modify 2", dtLoad.Rows[1][1, DataRowVersion.Current]);
            Assert.Equal("mono 2", dtLoad.Rows[1][1, DataRowVersion.Original]);
            Assert.Equal(DataRowState.Modified, dtLoad.Rows[1].RowState);
            // asserting Deleted Row2
            Assert.Equal("mono 3", dtLoad.Rows[2][1, DataRowVersion.Original]);
            Assert.Equal(DataRowState.Deleted, dtLoad.Rows[2].RowState);
            // asserting Added Row3
            Assert.Equal("Add 4", dtLoad.Rows[3][1, DataRowVersion.Current]);
            Assert.Equal("mono 4", dtLoad.Rows[3][1, DataRowVersion.Original]);
            Assert.Equal(DataRowState.Modified, dtLoad.Rows[3].RowState);
            // asserting Unpresent Row4
            Assert.Equal("mono 5", dtLoad.Rows[4][1, DataRowVersion.Current]);
            Assert.Equal("mono 5", dtLoad.Rows[4][1, DataRowVersion.Original]);
            Assert.Equal(DataRowState.Unchanged, dtLoad.Rows[4].RowState);
        }

        [Fact]
        public void Load_RowStateChangesDefaultDelete()
        {
            LocalSetup();
            DataTable dtLoad = new DataTable("LoadRowStateChanges");
            dtLoad.Columns.Add("id", typeof(int));
            dtLoad.Columns.Add("name", typeof(string));
            dtLoad.PrimaryKey = new DataColumn[] { dtLoad.Columns["id"] };
            dtLoad.Rows.Add(new object[] { 1, "RowState 1" });
            dtLoad.Rows.Add(new object[] { 2, "RowState 2" });
            dtLoad.Rows.Add(new object[] { 3, "RowState 3" });
            dtLoad.AcceptChanges();
            dtLoad.Rows[2].Delete();
            DataTableReader dtr = _dt.CreateDataReader();
            dtLoad.Load(dtr);

            Assert.Throws<VersionNotFoundException>(() => dtLoad.Rows[2][1, DataRowVersion.Current]);
        }

        [Fact]
        public void Load_RowStatePreserveChanges()
        {
            LocalSetup();
            _dt.Rows.Add(new object[] { 4, "mono 4" });
            _dt.Rows.Add(new object[] { 5, "mono 5" });
            _dt.AcceptChanges();
            DataTableReader dtr = _dt.CreateDataReader();
            DataTable dtLoad = setupRowState();
            DataRowAction[] dra = new DataRowAction[] {
                DataRowAction.ChangeCurrentAndOriginal,
                DataRowAction.ChangeOriginal,
                DataRowAction.ChangeOriginal,
                DataRowAction.ChangeOriginal,
                DataRowAction.ChangeCurrentAndOriginal};
            rowActionInit(dra);
            dtLoad.Load(dtr, LoadOption.PreserveChanges);
            rowActionEnd();
            // asserting Unchanged Row0
            Assert.Equal("mono 1", dtLoad.Rows[0][1, DataRowVersion.Current]);
            Assert.Equal("mono 1", dtLoad.Rows[0][1, DataRowVersion.Original]);
            Assert.Equal(DataRowState.Unchanged, dtLoad.Rows[0].RowState);
            // asserting Modified Row1
            Assert.Equal("Modify 2", dtLoad.Rows[1][1, DataRowVersion.Current]);
            Assert.Equal("mono 2", dtLoad.Rows[1][1, DataRowVersion.Original]);
            Assert.Equal(DataRowState.Modified, dtLoad.Rows[1].RowState);
            // asserting Deleted Row2
            Assert.Equal("mono 3", dtLoad.Rows[2][1, DataRowVersion.Original]);
            Assert.Equal(DataRowState.Deleted, dtLoad.Rows[2].RowState);
            // asserting Added Row3
            Assert.Equal("Add 4", dtLoad.Rows[3][1, DataRowVersion.Current]);
            Assert.Equal("mono 4", dtLoad.Rows[3][1, DataRowVersion.Original]);
            Assert.Equal(DataRowState.Modified, dtLoad.Rows[3].RowState);
            // asserting Unpresent Row4
            Assert.Equal("mono 5", dtLoad.Rows[4][1, DataRowVersion.Current]);
            Assert.Equal("mono 5", dtLoad.Rows[4][1, DataRowVersion.Original]);
            Assert.Equal(DataRowState.Unchanged, dtLoad.Rows[4].RowState);
        }

        [Fact]
        public void Load_RowStatePreserveChangesDelete()
        {
            LocalSetup();
            DataTable dtLoad = new DataTable("LoadRowStateChanges");
            dtLoad.Columns.Add("id", typeof(int));
            dtLoad.Columns.Add("name", typeof(string));
            dtLoad.PrimaryKey = new DataColumn[] { dtLoad.Columns["id"] };
            dtLoad.Rows.Add(new object[] { 1, "RowState 1" });
            dtLoad.Rows.Add(new object[] { 2, "RowState 2" });
            dtLoad.Rows.Add(new object[] { 3, "RowState 3" });
            dtLoad.AcceptChanges();
            dtLoad.Rows[2].Delete();
            DataTableReader dtr = _dt.CreateDataReader();
            dtLoad.Load(dtr, LoadOption.PreserveChanges);

            Assert.Throws<VersionNotFoundException>(() => dtLoad.Rows[2][1, DataRowVersion.Current]);
        }

        [Fact]
        public void Load_RowStateOverwriteChanges()
        {
            LocalSetup();
            _dt.Rows.Add(new object[] { 4, "mono 4" });
            _dt.Rows.Add(new object[] { 5, "mono 5" });
            _dt.AcceptChanges();
            DataTableReader dtr = _dt.CreateDataReader();
            DataTable dtLoad = setupRowState();
            DataRowAction[] dra = new DataRowAction[] {
                DataRowAction.ChangeCurrentAndOriginal,
                DataRowAction.ChangeCurrentAndOriginal,
                DataRowAction.ChangeCurrentAndOriginal,
                DataRowAction.ChangeCurrentAndOriginal,
                DataRowAction.ChangeCurrentAndOriginal};
            rowActionInit(dra);
            dtLoad.Load(dtr, LoadOption.OverwriteChanges);
            rowActionEnd();
            // asserting Unchanged Row0
            Assert.Equal("mono 1", dtLoad.Rows[0][1, DataRowVersion.Current]);
            Assert.Equal("mono 1", dtLoad.Rows[0][1, DataRowVersion.Original]);
            Assert.Equal(DataRowState.Unchanged, dtLoad.Rows[0].RowState);
            // asserting Modified Row1
            Assert.Equal("mono 2", dtLoad.Rows[1][1, DataRowVersion.Current]);
            Assert.Equal("mono 2", dtLoad.Rows[1][1, DataRowVersion.Original]);
            Assert.Equal(DataRowState.Unchanged, dtLoad.Rows[1].RowState);
            // asserting Deleted Row2
            Assert.Equal("mono 3", dtLoad.Rows[2][1, DataRowVersion.Current]);
            Assert.Equal("mono 3", dtLoad.Rows[2][1, DataRowVersion.Original]);
            Assert.Equal(DataRowState.Unchanged, dtLoad.Rows[2].RowState);
            // asserting Added Row3
            Assert.Equal("mono 4", dtLoad.Rows[3][1, DataRowVersion.Current]);
            Assert.Equal("mono 4", dtLoad.Rows[3][1, DataRowVersion.Original]);
            Assert.Equal(DataRowState.Unchanged, dtLoad.Rows[3].RowState);
            // asserting Unpresent Row4
            Assert.Equal("mono 5", dtLoad.Rows[4][1, DataRowVersion.Current]);
            Assert.Equal("mono 5", dtLoad.Rows[4][1, DataRowVersion.Original]);
            Assert.Equal(DataRowState.Unchanged, dtLoad.Rows[4].RowState);
        }

        [Fact]
        public void Load_RowStateUpsert()
        {
            LocalSetup();
            _dt.Rows.Add(new object[] { 4, "mono 4" });
            _dt.Rows.Add(new object[] { 5, "mono 5" });
            _dt.AcceptChanges();
            DataTableReader dtr = _dt.CreateDataReader();
            DataTable dtLoad = setupRowState();
            // Notice rowChange-Actions only occur 5 times, as number
            // of actual rows, ignoring row duplication of the deleted row.
            DataRowAction[] dra = new DataRowAction[] {
                DataRowAction.Change,
                DataRowAction.Change,
                DataRowAction.Add,
                DataRowAction.Change,
                DataRowAction.Add};
            rowActionInit(dra);
            dtLoad.Load(dtr, LoadOption.Upsert);
            rowActionEnd();
            // asserting Unchanged Row0
            Assert.Equal("mono 1", dtLoad.Rows[0][1, DataRowVersion.Current]);
            Assert.Equal("RowState 1", dtLoad.Rows[0][1, DataRowVersion.Original]);
            Assert.Equal(DataRowState.Modified, dtLoad.Rows[0].RowState);
            // asserting Modified Row1
            Assert.Equal("mono 2", dtLoad.Rows[1][1, DataRowVersion.Current]);
            Assert.Equal("RowState 2", dtLoad.Rows[1][1, DataRowVersion.Original]);
            Assert.Equal(DataRowState.Modified, dtLoad.Rows[1].RowState);
            // asserting Deleted Row2 and "Deleted-Added" Row4
            Assert.Equal("RowState 3", dtLoad.Rows[2][1, DataRowVersion.Original]);
            Assert.Equal(DataRowState.Deleted, dtLoad.Rows[2].RowState);
            Assert.Equal("mono 3", dtLoad.Rows[4][1, DataRowVersion.Current]);
            Assert.Equal(DataRowState.Added, dtLoad.Rows[4].RowState);
            // asserting Added Row3
            Assert.Equal("mono 4", dtLoad.Rows[3][1, DataRowVersion.Current]);
            Assert.Equal(DataRowState.Added, dtLoad.Rows[3].RowState);
            // asserting Unpresent Row5
            // Notice row4 is used for added row of deleted row2 and so
            // unpresent row4 moves to row5
            Assert.Equal("mono 5", dtLoad.Rows[5][1, DataRowVersion.Current]);
            Assert.Equal(DataRowState.Added, dtLoad.Rows[5].RowState);
        }

        [Fact]
        public void Load_RowStateUpsertDuplicateKey1()
        {
            LocalSetup();
            _dt.Rows.Add(new object[] { 4, "mono 4" });
            DataTable dtLoad = new DataTable("LoadRowStateChanges");
            dtLoad.Columns.Add("id", typeof(int));
            dtLoad.Columns.Add("name", typeof(string));
            dtLoad.PrimaryKey = new DataColumn[] { dtLoad.Columns["id"] };
            dtLoad.Rows.Add(new object[] { 1, "RowState 1" });
            dtLoad.Rows.Add(new object[] { 2, "RowState 2" });
            dtLoad.Rows.Add(new object[] { 3, "RowState 3" });
            dtLoad.AcceptChanges();
            dtLoad.Rows[2].Delete();
            DataTableReader dtr = _dt.CreateDataReader();
            dtLoad.Load(dtr, LoadOption.Upsert);
            dtLoad.Rows[3][1] = "NEWVAL";
            Assert.Equal(DataRowState.Deleted, dtLoad.Rows[2].RowState);
            Assert.Equal(3, dtLoad.Rows[2][0, DataRowVersion.Original]);
            Assert.Equal("RowState 3", dtLoad.Rows[2][1, DataRowVersion.Original]);
            Assert.Equal(DataRowState.Added, dtLoad.Rows[3].RowState);
            Assert.Equal(3, dtLoad.Rows[3][0, DataRowVersion.Current]);
            Assert.Equal("NEWVAL", dtLoad.Rows[3][1, DataRowVersion.Current]);
            Assert.Equal(DataRowState.Added, dtLoad.Rows[4].RowState);
            Assert.Equal(4, dtLoad.Rows[4][0, DataRowVersion.Current]);
            Assert.Equal("mono 4", dtLoad.Rows[4][1, DataRowVersion.Current]);

            dtLoad.AcceptChanges();

            Assert.Equal(DataRowState.Unchanged, dtLoad.Rows[2].RowState);
            Assert.Equal(3, dtLoad.Rows[2][0, DataRowVersion.Current]);
            Assert.Equal("NEWVAL", dtLoad.Rows[2][1, DataRowVersion.Current]);
            Assert.Equal(DataRowState.Unchanged, dtLoad.Rows[3].RowState);
            Assert.Equal(4, dtLoad.Rows[3][0, DataRowVersion.Current]);
            Assert.Equal("mono 4", dtLoad.Rows[3][1, DataRowVersion.Current]);
        }

        [Fact]
        public void Load_RowStateUpsertDuplicateKey2()
        {
            LocalSetup();
            _dt.Rows.Add(new object[] { 4, "mono 4" });
            DataTable dtLoad = new DataTable("LoadRowStateChanges");
            dtLoad.Columns.Add("id", typeof(int));
            dtLoad.Columns.Add("name", typeof(string));
            dtLoad.PrimaryKey = new DataColumn[] { dtLoad.Columns["id"] };
            dtLoad.Rows.Add(new object[] { 1, "RowState 1" });
            dtLoad.Rows.Add(new object[] { 2, "RowState 2" });
            dtLoad.Rows.Add(new object[] { 3, "RowState 3" });
            dtLoad.AcceptChanges();
            dtLoad.Rows[2].Delete();
            DataTableReader dtr = _dt.CreateDataReader();
            dtLoad.Load(dtr, LoadOption.Upsert);
            dtLoad.AcceptChanges();

            Assert.Throws<IndexOutOfRangeException>(() => dtLoad.Rows[4][1]);
        }

        [Fact]
        public void Load_RowStateUpsertDelete1()
        {
            LocalSetup();
            DataTable dtLoad = new DataTable("LoadRowStateChanges");
            dtLoad.Columns.Add("id", typeof(int));
            dtLoad.Columns.Add("name", typeof(string));
            dtLoad.PrimaryKey = new DataColumn[] { dtLoad.Columns["id"] };
            dtLoad.Rows.Add(new object[] { 1, "RowState 1" });
            dtLoad.Rows.Add(new object[] { 2, "RowState 2" });
            dtLoad.Rows.Add(new object[] { 3, "RowState 3" });
            dtLoad.AcceptChanges();
            dtLoad.Rows[2].Delete();
            DataTableReader dtr = _dt.CreateDataReader();
            dtLoad.Load(dtr, LoadOption.Upsert);

            Assert.Throws<VersionNotFoundException>(() => dtLoad.Rows[2][1, DataRowVersion.Current]);
        }

        [Fact]
        public void Load_RowStateUpsertDelete2()
        {
            LocalSetup();
            DataTable dtLoad = new DataTable("LoadRowStateChanges");
            dtLoad.Columns.Add("id", typeof(int));
            dtLoad.Columns.Add("name", typeof(string));
            dtLoad.PrimaryKey = new DataColumn[] { dtLoad.Columns["id"] };
            dtLoad.Rows.Add(new object[] { 1, "RowState 1" });
            dtLoad.Rows.Add(new object[] { 2, "RowState 2" });
            dtLoad.Rows.Add(new object[] { 3, "RowState 3" });
            dtLoad.AcceptChanges();
            dtLoad.Rows[2].Delete();
            DataTableReader dtr = _dt.CreateDataReader();
            dtLoad.Load(dtr, LoadOption.Upsert);

            Assert.Throws<VersionNotFoundException>(() => dtLoad.Rows[3][1, DataRowVersion.Original]);
        }

        [Fact]
        public void Load_RowStateUpsertAdd()
        {
            LocalSetup();
            _dt.Rows.Add(new object[] { 4, "mono 4" });
            DataTable dtLoad = new DataTable("LoadRowStateChanges");
            dtLoad.Columns.Add("id", typeof(int));
            dtLoad.Columns.Add("name", typeof(string));
            dtLoad.PrimaryKey = new DataColumn[] { dtLoad.Columns["id"] };
            dtLoad.Rows.Add(new object[] { 1, "RowState 1" });
            dtLoad.Rows.Add(new object[] { 2, "RowState 2" });
            dtLoad.Rows.Add(new object[] { 3, "RowState 3" });
            dtLoad.AcceptChanges();
            DataRow row = dtLoad.NewRow();
            row["id"] = 4;
            row["name"] = "Add 4";
            dtLoad.Rows.Add(row);
            DataTableReader dtr = _dt.CreateDataReader();
            dtLoad.Load(dtr, LoadOption.Upsert);

            Assert.Throws<VersionNotFoundException>(() => dtLoad.Rows[3][1, DataRowVersion.Original]);
        }

        [Fact]
        public void Load_RowStateUpsertUnpresent()
        {
            LocalSetup();
            _dt.Rows.Add(new object[] { 4, "mono 4" });
            DataTable dtLoad = new DataTable("LoadRowStateChanges");
            dtLoad.Columns.Add("id", typeof(int));
            dtLoad.Columns.Add("name", typeof(string));
            dtLoad.PrimaryKey = new DataColumn[] { dtLoad.Columns["id"] };
            dtLoad.Rows.Add(new object[] { 1, "RowState 1" });
            dtLoad.Rows.Add(new object[] { 2, "RowState 2" });
            dtLoad.Rows.Add(new object[] { 3, "RowState 3" });
            dtLoad.AcceptChanges();
            DataTableReader dtr = _dt.CreateDataReader();
            dtLoad.Load(dtr, LoadOption.Upsert);

            Assert.Throws<VersionNotFoundException>(() => dtLoad.Rows[3][1, DataRowVersion.Original]);
        }

        [Fact]
        public void Load_RowStateUpsertUnchangedEqualVal()
        {
            LocalSetup();
            DataTable dtLoad = new DataTable("LoadRowStateChanges");
            dtLoad.Columns.Add("id", typeof(int));
            dtLoad.Columns.Add("name", typeof(string));
            dtLoad.PrimaryKey = new DataColumn[] { dtLoad.Columns["id"] };
            dtLoad.Rows.Add(new object[] { 1, "mono 1" });
            dtLoad.AcceptChanges();
            DataTableReader dtr = _dt.CreateDataReader();
            DataRowAction[] dra = new DataRowAction[] {
                DataRowAction.Nothing,// REAL action
                DataRowAction.Nothing,// dummy
                DataRowAction.Nothing,// dummy
                DataRowAction.Nothing,// dummy
                DataRowAction.Nothing};// dummy
            rowActionInit(dra);
            dtLoad.Load(dtr, LoadOption.Upsert);
            rowActionEnd();
            Assert.Equal("mono 1", dtLoad.Rows[0][1, DataRowVersion.Current]);
            Assert.Equal("mono 1", dtLoad.Rows[0][1, DataRowVersion.Original]);
            Assert.Equal(DataRowState.Unchanged, dtLoad.Rows[0].RowState);
        }

        [Fact]
        public void LoadDataRow_LoadOptions()
        {
            // LoadDataRow is covered in detail (without LoadOptions) in DataTableTest2
            // LoadOption tests are covered in detail in DataTable.Load().
            // Therefore only minimal tests of LoadDataRow with LoadOptions are covered here.
            DataTable dt;
            DataRow dr;
            dt = CreateDataTableExample();
            dt.PrimaryKey = new DataColumn[] { dt.Columns[0] }; //add ParentId as Primary Key
            dt.Columns["String1"].DefaultValue = "Default";

            dr = dt.Select("ParentId=1")[0];

            //Update existing row with LoadOptions = OverwriteChanges
            dt.BeginLoadData();
            dt.LoadDataRow(new object[] { 1, null, "Changed" },
                LoadOption.OverwriteChanges);
            dt.EndLoadData();

            // LoadDataRow(update1) - check column String2
            Assert.Equal("Changed", dr["String2", DataRowVersion.Current]);
            Assert.Equal("Changed", dr["String2", DataRowVersion.Original]);

            // LoadDataRow(update1) - check row state
            Assert.Equal(DataRowState.Unchanged, dr.RowState);

            //Add New row with LoadOptions = Upsert
            dt.BeginLoadData();
            dt.LoadDataRow(new object[] { 99, null, "Changed" },
                LoadOption.Upsert);
            dt.EndLoadData();

            // LoadDataRow(insert1) - check column String2
            dr = dt.Select("ParentId=99")[0];
            Assert.Equal("Changed", dr["String2", DataRowVersion.Current]);

            // LoadDataRow(insert1) - check row state
            Assert.Equal(DataRowState.Added, dr.RowState);
        }

        public static DataTable CreateDataTableExample()
        {
            DataTable dtParent = new DataTable("Parent");

            dtParent.Columns.Add("ParentId", typeof(int));
            dtParent.Columns.Add("String1", typeof(string));
            dtParent.Columns.Add("String2", typeof(string));

            dtParent.Columns.Add("ParentDateTime", typeof(DateTime));
            dtParent.Columns.Add("ParentDouble", typeof(double));
            dtParent.Columns.Add("ParentBool", typeof(bool));

            dtParent.Rows.Add(new object[] { 1, "1-String1", "1-String2", new DateTime(2005, 1, 1, 0, 0, 0, 0), 1.534, true });
            dtParent.Rows.Add(new object[] { 2, "2-String1", "2-String2", new DateTime(2004, 1, 1, 0, 0, 0, 1), -1.534, true });
            dtParent.Rows.Add(new object[] { 3, "3-String1", "3-String2", new DateTime(2003, 1, 1, 0, 0, 1, 0), double.MinValue * 10000, false });
            dtParent.Rows.Add(new object[] { 4, "4-String1", "4-String2", new DateTime(2002, 1, 1, 0, 1, 0, 0), double.MaxValue / 10000, true });
            dtParent.Rows.Add(new object[] { 5, "5-String1", "5-String2", new DateTime(2001, 1, 1, 1, 0, 0, 0), 0.755, true });
            dtParent.Rows.Add(new object[] { 6, "6-String1", "6-String2", new DateTime(2000, 1, 1, 0, 0, 0, 0), 0.001, false });
            dtParent.AcceptChanges();
            return dtParent;
        }

        #endregion // DataTable.Load Tests

        #region Read/Write XML Tests

        [Fact]
        public void ReadXmlSchema()
        {
            DataTable Table = new DataTable();
            Table.ReadXmlSchema(new StringReader(DataProvider.own_schema1));

            Assert.Equal("test_table", Table.TableName);
            Assert.Equal("", Table.Namespace);
            Assert.Equal(2, Table.Columns.Count);
            Assert.Equal(0, Table.Rows.Count);
            Assert.False(Table.CaseSensitive);
            Assert.Equal(1, Table.Constraints.Count);
            Assert.Equal("", Table.Prefix);

            Constraint cons = Table.Constraints[0];
            Assert.Equal("Constraint1", cons.ConstraintName.ToString());
            Assert.Equal("Constraint1", cons.ToString());

            DataColumn column = Table.Columns[0];
            Assert.True(column.AllowDBNull);
            Assert.False(column.AutoIncrement);
            Assert.Equal(0L, column.AutoIncrementSeed);
            Assert.Equal(1L, column.AutoIncrementStep);
            Assert.Equal("test", column.Caption);
            Assert.Equal("Element", column.ColumnMapping.ToString());
            Assert.Equal("first", column.ColumnName);
            Assert.Equal(typeof(string), column.DataType);
            Assert.Equal("test_default_value", column.DefaultValue.ToString());
            Assert.False(column.DesignMode);
            Assert.Equal("", column.Expression);
            Assert.Equal(100, column.MaxLength);
            Assert.Equal("", column.Namespace);
            Assert.Equal(0, column.Ordinal);
            Assert.Equal("", column.Prefix);
            Assert.False(column.ReadOnly);
            Assert.True(column.Unique);

            DataColumn column2 = Table.Columns[1];
            Assert.True(column2.AllowDBNull);
            Assert.False(column2.AutoIncrement);
            Assert.Equal(0L, column2.AutoIncrementSeed);
            Assert.Equal(1L, column2.AutoIncrementStep);
            Assert.Equal("second", column2.Caption);
            Assert.Equal("Element", column2.ColumnMapping.ToString());
            Assert.Equal("second", column2.ColumnName);
            Assert.Equal(typeof(SqlGuid), column2.DataType);
            Assert.Equal(SqlGuid.Null, column2.DefaultValue);
            Assert.Equal(typeof(SqlGuid), column2.DefaultValue.GetType());
            Assert.False(column2.DesignMode);
            Assert.Equal("", column2.Expression);
            Assert.Equal(-1, column2.MaxLength);
            Assert.Equal("", column2.Namespace);
            Assert.Equal(1, column2.Ordinal);
            Assert.Equal("", column2.Prefix);
            Assert.False(column2.ReadOnly);
            Assert.False(column2.Unique);

            DataTable Table2 = new DataTable();
            Table2.ReadXmlSchema(new StringReader(DataProvider.own_schema2));

            Assert.Equal("second_test_table", Table2.TableName);
            Assert.Equal("", Table2.Namespace);
            Assert.Equal(1, Table2.Columns.Count);
            Assert.Equal(0, Table2.Rows.Count);
            Assert.False(Table2.CaseSensitive);
            Assert.Equal(1, Table2.Constraints.Count);
            Assert.Equal("", Table2.Prefix);

            DataColumn column3 = Table2.Columns[0];
            Assert.True(column3.AllowDBNull);
            Assert.False(column3.AutoIncrement);
            Assert.Equal(0L, column3.AutoIncrementSeed);
            Assert.Equal(1L, column3.AutoIncrementStep);
            Assert.Equal("second_first", column3.Caption);
            Assert.Equal("Element", column3.ColumnMapping.ToString());
            Assert.Equal("second_first", column3.ColumnName);
            Assert.Equal(typeof(string), column3.DataType);
            Assert.Equal("default_value", column3.DefaultValue.ToString());
            Assert.False(column3.DesignMode);
            Assert.Equal("", column3.Expression);
            Assert.Equal(100, column3.MaxLength);
            Assert.Equal("", column3.Namespace);
            Assert.Equal(0, column3.Ordinal);
            Assert.Equal("", column3.Prefix);
            Assert.False(column3.ReadOnly);
            Assert.True(column3.Unique);
        }

        [ConditionalFact(typeof(PlatformDetection), nameof(PlatformDetection.IsNotInvariantGlobalization))]
        public void ReadXmlSchema_2()
        {
            DataTable dt = new DataTable();
            string xmlData = string.Empty;
            xmlData += "<?xml version=\"1.0\"?>";
            xmlData += "<xs:schema id=\"SiteConfiguration\" targetNamespace=\"http://tempuri.org/PortalCfg.xsd\" xmlns:mstns=\"http://tempuri.org/PortalCfg.xsd\" xmlns=\"http://tempuri.org/PortalCfg.xsd\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:msdata=\"urn:schemas-microsoft-com:xml-msdata\" attributeFormDefault=\"qualified\" elementFormDefault=\"qualified\">";
            xmlData += "<xs:element name=\"SiteConfiguration\" msdata:IsDataSet=\"true\" msdata:EnforceConstraints=\"False\">";
            xmlData += "<xs:complexType>";
            xmlData += "<xs:choice  minOccurs=\"0\" maxOccurs=\"unbounded\">";
            xmlData += "<xs:element name=\"Tab\">";
            xmlData += "<xs:complexType>";
            xmlData += "<xs:sequence>";
            xmlData += "<xs:element name=\"Module\" minOccurs=\"0\" maxOccurs=\"unbounded\">";
            xmlData += "<xs:complexType>";
            xmlData += "<xs:attribute name=\"ModuleId\" form=\"unqualified\" type=\"xs:int\" />";
            xmlData += "</xs:complexType>";
            xmlData += "</xs:element>";
            xmlData += "</xs:sequence>";
            xmlData += "<xs:attribute name=\"TabId\" form=\"unqualified\" type=\"xs:int\" />";
            xmlData += "</xs:complexType>";
            xmlData += "</xs:element>";
            xmlData += "</xs:choice>";
            xmlData += "</xs:complexType>";
            xmlData += "<xs:key name=\"TabKey\" msdata:PrimaryKey=\"true\">";
            xmlData += "<xs:selector xpath=\".//mstns:Tab\" />";
            xmlData += "<xs:field xpath=\"@TabId\" />";
            xmlData += "</xs:key>";
            xmlData += "<xs:key name=\"ModuleKey\" msdata:PrimaryKey=\"true\">";
            xmlData += "<xs:selector xpath=\".//mstns:Module\" />";
            xmlData += "<xs:field xpath=\"@ModuleID\" />";
            xmlData += "</xs:key>";
            xmlData += "</xs:element>";
            xmlData += "</xs:schema>";
            dt.ReadXmlSchema(new StringReader(xmlData));
        }

        [Fact]
        public void ReadXmlSchema_ByStream()
        {
            DataSet ds1 = new DataSet();
            ds1.Tables.Add(DataProvider.CreateParentDataTable());
            ds1.Tables.Add(DataProvider.CreateChildDataTable());

            MemoryStream ms1 = new MemoryStream();
            MemoryStream ms2 = new MemoryStream();
            //write xml  schema only
            //ds1.WriteXmlSchema (ms);
            ds1.Tables[0].WriteXmlSchema(ms1);
            ds1.Tables[1].WriteXmlSchema(ms2);

            MemoryStream ms11 = new MemoryStream(ms1.GetBuffer());
            MemoryStream ms22 = new MemoryStream(ms2.GetBuffer());
            //copy schema
            //DataSet ds2 = new DataSet ();
            DataTable dt1 = new DataTable();
            DataTable dt2 = new DataTable();

            //ds2.ReadXmlSchema (ms1);
            dt1.ReadXmlSchema(ms11);
            dt2.ReadXmlSchema(ms22);

            //check xml schema
            // ReadXmlSchema - Tables count
            //Assert.Equal (ds2.Tables.Count, ds1.Tables.Count);

            // ReadXmlSchema - Tables 0 Col count
            Assert.Equal(ds1.Tables[0].Columns.Count, dt1.Columns.Count);

            // ReadXmlSchema - Tables 1 Col count
            Assert.Equal(ds1.Tables[1].Columns.Count, dt2.Columns.Count);

            //check some columns types
            // ReadXmlSchema - Tables 0 Col type
            Assert.Equal(ds1.Tables[0].Columns[0].GetType(), dt1.Columns[0].GetType());

            // ReadXmlSchema - Tables 1 Col type
            Assert.Equal(ds1.Tables[1].Columns[3].GetType(), dt2.Columns[3].GetType());

            //check that no data exists
            // ReadXmlSchema - Table 1 row count
            Assert.Equal(0, dt1.Rows.Count);

            // ReadXmlSchema - Table 2 row count
            Assert.Equal(0, dt2.Rows.Count);
        }

        [Fact]
        public void ReadWriteXmlSchema_ByFileName()
        {
            string sTempFileName1 = Path.Combine(Path.GetTempPath(), "tmpDataSet_ReadWriteXml_43899-1.xml");
            string sTempFileName2 = Path.Combine(Path.GetTempPath(), "tmpDataSet_ReadWriteXml_43899-2.xml");

            DataSet ds1 = new DataSet();
            ds1.Tables.Add(DataProvider.CreateParentDataTable());
            ds1.Tables.Add(DataProvider.CreateChildDataTable());

            ds1.Tables[0].WriteXmlSchema(sTempFileName1);
            ds1.Tables[1].WriteXmlSchema(sTempFileName2);

            DataTable dt1 = new DataTable();
            DataTable dt2 = new DataTable();

            dt1.ReadXmlSchema(sTempFileName1);
            dt2.ReadXmlSchema(sTempFileName2);

            Assert.Equal(ds1.Tables[0].Columns.Count, dt1.Columns.Count);
            Assert.Equal(ds1.Tables[1].Columns.Count, dt2.Columns.Count);
            Assert.Equal(ds1.Tables[0].Columns[0].GetType(), dt1.Columns[0].GetType());
            Assert.Equal(ds1.Tables[1].Columns[3].GetType(), dt2.Columns[3].GetType());
            Assert.Equal(0, dt1.Rows.Count);
            Assert.Equal(0, dt2.Rows.Count);

            File.Delete(sTempFileName1);
            File.Delete(sTempFileName2);
        }

        [Fact]
        public void ReadXmlSchema_ByTextReader()
        {
            DataSet ds1 = new DataSet();
            ds1.Tables.Add(DataProvider.CreateParentDataTable());
            ds1.Tables.Add(DataProvider.CreateChildDataTable());

            StringWriter sw1 = new StringWriter();
            StringWriter sw2 = new StringWriter();
            //write xml file, schema only
            //ds1.WriteXmlSchema (sw);
            ds1.Tables[0].WriteXmlSchema(sw1);
            ds1.Tables[1].WriteXmlSchema(sw2);

            StringReader sr1 = new StringReader(sw1.GetStringBuilder().ToString());
            StringReader sr2 = new StringReader(sw2.GetStringBuilder().ToString());
            //copy both data and schema
            //DataSet ds2 = new DataSet ();
            DataTable dt1 = new DataTable();
            DataTable dt2 = new DataTable();

            //ds2.ReadXmlSchema (sr);
            dt1.ReadXmlSchema(sr1);
            dt2.ReadXmlSchema(sr2);

            //check xml schema
            // ReadXmlSchema - Tables count
            //Assert.Equal (ds2.Tables.Count, ds1.Tables.Count);

            // ReadXmlSchema - Tables 0 Col count
            Assert.Equal(ds1.Tables[0].Columns.Count, dt1.Columns.Count);

            // ReadXmlSchema - Tables 1 Col count
            Assert.Equal(ds1.Tables[1].Columns.Count, dt2.Columns.Count);

            //check some columns types
            // ReadXmlSchema - Tables 0 Col type
            Assert.Equal(ds1.Tables[0].Columns[0].GetType(), dt1.Columns[0].GetType());

            // ReadXmlSchema - Tables 1 Col type
            Assert.Equal(ds1.Tables[1].Columns[3].GetType(), dt2.Columns[3].GetType());

            //check that no data exists
            // ReadXmlSchema - Table 1 row count
            Assert.Equal(0, dt1.Rows.Count);

            // ReadXmlSchema - Table 2 row count
            Assert.Equal(0, dt2.Rows.Count);
        }

        [Fact]
        public void ReadXmlSchema_ByXmlReader()
        {
            DataSet ds1 = new DataSet();
            ds1.Tables.Add(DataProvider.CreateParentDataTable());
            ds1.Tables.Add(DataProvider.CreateChildDataTable());

            StringWriter sw1 = new StringWriter();
            XmlTextWriter xmlTW1 = new XmlTextWriter(sw1);
            StringWriter sw2 = new StringWriter();
            XmlTextWriter xmlTW2 = new XmlTextWriter(sw2);

            //write xml file, schema only
            ds1.Tables[0].WriteXmlSchema(xmlTW1);
            xmlTW1.Flush();
            ds1.Tables[1].WriteXmlSchema(xmlTW2);
            xmlTW2.Flush();

            StringReader sr1 = new StringReader(sw1.ToString());
            XmlTextReader xmlTR1 = new XmlTextReader(sr1);
            StringReader sr2 = new StringReader(sw2.ToString());
            XmlTextReader xmlTR2 = new XmlTextReader(sr2);

            //copy both data and schema
            //DataSet ds2 = new DataSet ();
            DataTable dt1 = new DataTable();
            DataTable dt2 = new DataTable();

            //ds2.ReadXmlSchema (xmlTR);
            dt1.ReadXmlSchema(xmlTR1);
            dt2.ReadXmlSchema(xmlTR2);

            //check xml schema
            // ReadXmlSchema - Tables count
            //Assert.Equal (ds2.Tables.Count, ds1.Tables.Count);

            // ReadXmlSchema - Tables 0 Col count
            Assert.Equal(ds1.Tables[0].Columns.Count, dt1.Columns.Count);

            // ReadXmlSchema - Tables 1 Col count
            Assert.Equal(ds1.Tables[1].Columns.Count, dt2.Columns.Count);

            //check some columns types
            // ReadXmlSchema - Tables 0 Col type
            Assert.Equal(ds1.Tables[0].Columns[0].GetType(), dt1.Columns[0].GetType());

            // ReadXmlSchema - Tables 1 Col type
            Assert.Equal(ds1.Tables[1].Columns[3].GetType(), dt2.Columns[3].GetType());

            //check that no data exists
            // ReadXmlSchema - Table 1 row count
            Assert.Equal(0, dt1.Rows.Count);

            // ReadXmlSchema - Table 2 row count
            Assert.Equal(0, dt2.Rows.Count);
        }

        [ConditionalFact(typeof(PlatformDetection), nameof(PlatformDetection.IsNotInvariantGlobalization))]
        public void WriteXmlSchema()
        {
            using (new ThreadCultureChange("en-GB"))
            {
                var ds = new DataSet();
                ds.ReadXml(new StringReader(DataProvider.region));
                TextWriter writer = new StringWriter();
                ds.Tables[0].WriteXmlSchema(writer);

                // Looks like whoever added this test depended on en-US culture (see 'msdata:Locale attr' in 3rd line below), which is wrong.
                string expected = @"<?xml version=""1.0"" encoding=""utf-16""?>
<xs:schema id=""Root"" xmlns:msdata=""urn:schemas-microsoft-com:xml-msdata"" xmlns:xs=""http://www.w3.org/2001/XMLSchema"">
  <xs:element msdata:IsDataSet=""true"" msdata:Locale=""en-US"" msdata:MainDataTable=""Region"" name=""Root"">
    <xs:complexType>
      <xs:choice maxOccurs=""unbounded"" minOccurs=""0"">
        <xs:element name=""Region"">
          <xs:complexType>
            <xs:sequence>
              <xs:element minOccurs=""0"" name=""RegionID"" type=""xs:string"" />
              <xs:element minOccurs=""0"" name=""RegionDescription"" type=""xs:string"" />
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:choice>
    </xs:complexType>
  </xs:element>
</xs:schema>".ReplaceLineEndings();

                string textString = DataSetAssertion.GetNormalizedSchema(writer.ToString());
                Assert.Equal(expected, textString.ReplaceLineEndings());
            }
        }

        [Fact]
        public void WriteXmlSchema2()
        {
            string xml = @"<myDataSet xmlns='NetFrameWork'><myTable><id>0</id><item>item 0</item></myTable><myTable><id>1</id><item>item 1</item></myTable><myTable><id>2</id><item>item 2</item></myTable><myTable><id>3</id><item>item 3</item></myTable><myTable><id>4</id><item>item 4</item></myTable><myTable><id>5</id><item>item 5</item></myTable><myTable><id>6</id><item>item 6</item></myTable><myTable><id>7</id><item>item 7</item></myTable><myTable><id>8</id><item>item 8</item></myTable><myTable><id>9</id><item>item 9</item></myTable></myDataSet>";
            string schema = @"<?xml version='1.0' encoding='utf-16'?>
<xs:schema id='myDataSet' targetNamespace='NetFrameWork' xmlns:mstns='NetFrameWork' xmlns='NetFrameWork' xmlns:xs='http://www.w3.org/2001/XMLSchema' xmlns:msdata='urn:schemas-microsoft-com:xml-msdata' attributeFormDefault='qualified' elementFormDefault='qualified'>
  <xs:element name='myDataSet' msdata:IsDataSet='true' msdata:MainDataTable='NetFrameWork_x003A_myTable' msdata:UseCurrentLocale='true'>
    <xs:complexType>
      <xs:choice minOccurs='0' maxOccurs='unbounded'>
        <xs:element name='myTable'>
          <xs:complexType>
            <xs:sequence>
              <xs:element name='id' msdata:AutoIncrement='true' type='xs:int' minOccurs='0' />
              <xs:element name='item' type='xs:string' minOccurs='0' />
            </xs:sequence>
          </xs:complexType>
        </xs:element>
      </xs:choice>
    </xs:complexType>
  </xs:element>
</xs:schema>";
            DataSet OriginalDataSet = new DataSet("myDataSet");
            OriginalDataSet.Namespace = "NetFrameWork";
            DataTable myTable = new DataTable("myTable");
            DataColumn c1 = new DataColumn("id", typeof(int));
            c1.AutoIncrement = true;
            DataColumn c2 = new DataColumn("item");
            myTable.Columns.Add(c1);
            myTable.Columns.Add(c2);
            OriginalDataSet.Tables.Add(myTable);
            // Add ten rows.
            DataRow newRow;
            for (int i = 0; i < 10; i++)
            {
                newRow = myTable.NewRow();
                newRow["item"] = "item " + i;
                myTable.Rows.Add(newRow);
            }
            OriginalDataSet.AcceptChanges();

            StringWriter sw = new StringWriter();
            XmlTextWriter xtw = new XmlTextWriter(sw);
            xtw.QuoteChar = '\'';
            OriginalDataSet.WriteXml(xtw);
            string result = sw.ToString();

            Assert.Equal(xml, result);

            sw = new StringWriter();
            xtw = new XmlTextWriter(sw);
            xtw.Formatting = Formatting.Indented;
            OriginalDataSet.Tables[0].WriteXmlSchema(xtw);
            result = sw.ToString();

            result = result.Replace("\r\n", "\n").Replace('"', '\'');
            Assert.Equal(schema.Replace("\r\n", "\n"), result);
        }

        [Fact]
        public void WriteXmlSchema3()
        {
            string xmlschema = @"<?xml version=""1.0"" encoding=""utf-16""?>
<xs:schema id=""ExampleDataSet"" xmlns="""" xmlns:xs=""http://www.w3.org/2001/XMLSchema"" xmlns:msdata=""urn:schemas-microsoft-com:xml-msdata"">
  <xs:element name=""ExampleDataSet"" msdata:IsDataSet=""true"" msdata:MainDataTable=""ExampleDataTable"" msdata:UseCurrentLocale=""true"">
    <xs:complexType>
      <xs:choice minOccurs=""0"" maxOccurs=""unbounded"">
        <xs:element name=""ExampleDataTable"">
          <xs:complexType>
            <xs:attribute name=""PrimaryKeyColumn"" type=""xs:int"" use=""required"" />
          </xs:complexType>
        </xs:element>
      </xs:choice>
    </xs:complexType>
    <xs:unique name=""PK_ExampleDataTable"" msdata:PrimaryKey=""true"">
      <xs:selector xpath="".//ExampleDataTable"" />
      <xs:field xpath=""@PrimaryKeyColumn"" />
    </xs:unique>
  </xs:element>
</xs:schema>";
            DataSet ds = new DataSet("ExampleDataSet");

            ds.Tables.Add(new DataTable("ExampleDataTable"));
            ds.Tables["ExampleDataTable"].Columns.Add(
                new DataColumn("PrimaryKeyColumn", typeof(int), "", MappingType.Attribute));
            ds.Tables["ExampleDataTable"].Columns["PrimaryKeyColumn"].AllowDBNull = false;

            ds.Tables["ExampleDataTable"].Constraints.Add(
                "PK_ExampleDataTable",
                ds.Tables["ExampleDataTable"].Columns["PrimaryKeyColumn"],
                true);

            ds.AcceptChanges();
            StringWriter sw = new StringWriter();
            ds.Tables[0].WriteXmlSchema(sw);

            string result = sw.ToString();

            Assert.Equal(xmlschema.Replace("\r\n", "\n"), result.Replace("\r\n", "\n"));
        }

        [Fact]
        public void WriteXmlSchema4()
        {
            string xmlschema = @"<?xml version=""1.0"" encoding=""utf-16""?>
<xs:schema id=""Example"" xmlns="""" xmlns:xs=""http://www.w3.org/2001/XMLSchema"" xmlns:msdata=""urn:schemas-microsoft-com:xml-msdata"">
  <xs:element name=""Example"" msdata:IsDataSet=""true"" msdata:MainDataTable=""MyType"" msdata:UseCurrentLocale=""true"">
    <xs:complexType>
      <xs:choice minOccurs=""0"" maxOccurs=""unbounded"">
        <xs:element name=""MyType"">
          <xs:complexType>
            <xs:attribute name=""ID"" type=""xs:int"" use=""required"" />
            <xs:attribute name=""Desc"" type=""xs:string"" />
          </xs:complexType>
        </xs:element>
      </xs:choice>
    </xs:complexType>
  </xs:element>
</xs:schema>";
            DataSet ds = new DataSet("Example");

            // Add MyType DataTable
            DataTable dt = new DataTable("MyType");
            ds.Tables.Add(dt);

            dt.Columns.Add(new DataColumn("ID", typeof(int), "",
                MappingType.Attribute));
            dt.Columns["ID"].AllowDBNull = false;

            dt.Columns.Add(new DataColumn("Desc", typeof
                (string), "", MappingType.Attribute));

            ds.AcceptChanges();

            StringWriter sw = new StringWriter();
            ds.Tables[0].WriteXmlSchema(sw);

            string result = sw.ToString();

            Assert.Equal(xmlschema.Replace("\r\n", "\n"), result.Replace("\r\n", "\n"));
        }

        [Fact]
        public void WriteXmlSchema5()
        {
            string xmlschema1 = @"<?xml version=""1.0"" encoding=""utf-16""?>
<xs:schema id=""Example"" xmlns="""" xmlns:xs=""http://www.w3.org/2001/XMLSchema"" xmlns:msdata=""urn:schemas-microsoft-com:xml-msdata"">
  <xs:element name=""Example"" msdata:IsDataSet=""true"" msdata:MainDataTable=""StandAlone"" msdata:UseCurrentLocale=""true"">
    <xs:complexType>
      <xs:choice minOccurs=""0"" maxOccurs=""unbounded"">
        <xs:element name=""StandAlone"">
          <xs:complexType>
            <xs:attribute name=""ID"" type=""xs:int"" use=""required"" />
            <xs:attribute name=""Desc"" type=""xs:string"" use=""required"" />
          </xs:complexType>
        </xs:element>
      </xs:choice>
    </xs:complexType>
  </xs:element>
</xs:schema>";
            string xmlschema2 = @"<?xml version=""1.0"" encoding=""utf-16""?>
<xs:schema id=""Example"" xmlns="""" xmlns:xs=""http://www.w3.org/2001/XMLSchema"" xmlns:msdata=""urn:schemas-microsoft-com:xml-msdata"">
  <xs:element name=""Example"" msdata:IsDataSet=""true"" msdata:MainDataTable=""Dimension"" msdata:UseCurrentLocale=""true"">
    <xs:complexType>
      <xs:choice minOccurs=""0"" maxOccurs=""unbounded"">
        <xs:element name=""Dimension"">
          <xs:complexType>
            <xs:attribute name=""Number"" msdata:ReadOnly=""true"" type=""xs:int"" use=""required"" />
            <xs:attribute name=""Title"" type=""xs:string"" use=""required"" />
          </xs:complexType>
        </xs:element>
      </xs:choice>
    </xs:complexType>
    <xs:unique name=""PK_Dimension"" msdata:PrimaryKey=""true"">
      <xs:selector xpath="".//Dimension"" />
      <xs:field xpath=""@Number"" />
    </xs:unique>
  </xs:element>
</xs:schema>";
            string xmlschema3 = @"<?xml version=""1.0"" encoding=""utf-16""?>
<xs:schema id=""Example"" xmlns="""" xmlns:xs=""http://www.w3.org/2001/XMLSchema"" xmlns:msdata=""urn:schemas-microsoft-com:xml-msdata"">
  <xs:element name=""Example"" msdata:IsDataSet=""true"" msdata:MainDataTable=""Element"" msdata:UseCurrentLocale=""true"">
    <xs:complexType>
      <xs:choice minOccurs=""0"" maxOccurs=""unbounded"">
        <xs:element name=""Element"">
          <xs:complexType>
            <xs:attribute name=""Dimension"" msdata:ReadOnly=""true"" type=""xs:int"" use=""required"" />
            <xs:attribute name=""Number"" msdata:ReadOnly=""true"" type=""xs:int"" use=""required"" />
            <xs:attribute name=""Title"" type=""xs:string"" use=""required"" />
          </xs:complexType>
        </xs:element>
      </xs:choice>
    </xs:complexType>
    <xs:unique name=""PK_Element"" msdata:PrimaryKey=""true"">
      <xs:selector xpath="".//Element"" />
      <xs:field xpath=""@Dimension"" />
      <xs:field xpath=""@Number"" />
    </xs:unique>
  </xs:element>
</xs:schema>";
            DataSet ds = new DataSet("Example");

            // Add a DataTable with no ReadOnly columns
            DataTable dt1 = new DataTable("StandAlone");
            ds.Tables.Add(dt1);

            // Add a ReadOnly column
            dt1.Columns.Add(new DataColumn("ID", typeof(int), "",
                MappingType.Attribute));
            dt1.Columns["ID"].AllowDBNull = false;

            dt1.Columns.Add(new DataColumn("Desc", typeof
                (string), "", MappingType.Attribute));
            dt1.Columns["Desc"].AllowDBNull = false;

            // Add related DataTables with ReadOnly columns
            DataTable dt2 = new DataTable("Dimension");
            ds.Tables.Add(dt2);
            dt2.Columns.Add(new DataColumn("Number", typeof
                (int), "", MappingType.Attribute));
            dt2.Columns["Number"].AllowDBNull = false;
            dt2.Columns["Number"].ReadOnly = true;

            dt2.Columns.Add(new DataColumn("Title", typeof
                (string), "", MappingType.Attribute));
            dt2.Columns["Title"].AllowDBNull = false;

            dt2.Constraints.Add("PK_Dimension", dt2.Columns["Number"], true);

            DataTable dt3 = new DataTable("Element");
            ds.Tables.Add(dt3);

            dt3.Columns.Add(new DataColumn("Dimension", typeof
                (int), "", MappingType.Attribute));
            dt3.Columns["Dimension"].AllowDBNull = false;
            dt3.Columns["Dimension"].ReadOnly = true;

            dt3.Columns.Add(new DataColumn("Number", typeof
                (int), "", MappingType.Attribute));
            dt3.Columns["Number"].AllowDBNull = false;
            dt3.Columns["Number"].ReadOnly = true;

            dt3.Columns.Add(new DataColumn("Title", typeof
                (string), "", MappingType.Attribute));
            dt3.Columns["Title"].AllowDBNull = false;

            dt3.Constraints.Add("PK_Element", new DataColumn[] {
                dt3.Columns ["Dimension"],
                dt3.Columns ["Number"] }, true);

            ds.AcceptChanges();

            StringWriter sw1 = new StringWriter();
            ds.Tables[0].WriteXmlSchema(sw1);
            string result1 = sw1.ToString();
            Assert.Equal(xmlschema1.Replace("\r\n", "\n"), result1.Replace("\r\n", "\n"));

            StringWriter sw2 = new StringWriter();
            ds.Tables[1].WriteXmlSchema(sw2);
            string result2 = sw2.ToString();
            Assert.Equal(xmlschema2.Replace("\r\n", "\n"), result2.Replace("\r\n", "\n"));

            StringWriter sw3 = new StringWriter();
            ds.Tables[2].WriteXmlSchema(sw3);
            string result3 = sw3.ToString();
            Assert.Equal(xmlschema3.Replace("\r\n", "\n"), result3.Replace("\r\n", "\n"));
        }

        [Fact]
        public void WriteXmlSchema6()
        {
            string xmlschema = @"<?xml version=""1.0"" encoding=""utf-16""?>
<xs:schema id=""Example"" xmlns="""" xmlns:xs=""http://www.w3.org/2001/XMLSchema"" xmlns:msdata=""urn:schemas-microsoft-com:xml-msdata"">
  <xs:element name=""Example"" msdata:IsDataSet=""true"" msdata:MainDataTable=""MyType"" msdata:UseCurrentLocale=""true"">
    <xs:complexType>
      <xs:choice minOccurs=""0"" maxOccurs=""unbounded"">
        <xs:element name=""MyType"">
          <xs:complexType>
            <xs:attribute name=""Desc"">
              <xs:simpleType>
                <xs:restriction base=""xs:string"">
                  <xs:maxLength value=""32"" />
                </xs:restriction>
              </xs:simpleType>
            </xs:attribute>
          </xs:complexType>
        </xs:element>
      </xs:choice>
    </xs:complexType>
  </xs:element>
</xs:schema>";
            DataSet ds = new DataSet("Example");

            // Add MyType DataTable
            ds.Tables.Add("MyType");

            ds.Tables["MyType"].Columns.Add(new DataColumn(
                "Desc", typeof(string), "", MappingType.Attribute));
            ds.Tables["MyType"].Columns["Desc"].MaxLength = 32;

            ds.AcceptChanges();

            StringWriter sw = new StringWriter();
            ds.Tables[0].WriteXmlSchema(sw);

            string result = sw.ToString();

            Assert.Equal(xmlschema.Replace("\r\n", "\n"), result.Replace("\r\n", "\n"));
        }

        [Fact]
        public void WriteXmlSchema7()
        {
            var ds = new DataSet();
            DataTable dt = new DataTable("table");
            dt.Columns.Add("col1");
            dt.Columns.Add("col2");
            ds.Tables.Add(dt);
            dt.Rows.Add(new object[] { "foo", "bar" });
            StringWriter sw = new StringWriter();
            ds.Tables[0].WriteXmlSchema(sw);
            Assert.True(sw.ToString().IndexOf("xmlns=\"\"") > 0);
        }

        [Fact]
        public void WriteXmlSchema_ConstraintNameWithSpaces()
        {
            var ds = new DataSet();
            DataTable table1 = ds.Tables.Add("table1");
            DataTable table2 = ds.Tables.Add("table2");

            table1.Columns.Add("col1", typeof(int));
            table2.Columns.Add("col1", typeof(int));

            table1.Constraints.Add("uc 1", table1.Columns[0], false);
            table2.Constraints.Add("fc 1", table1.Columns[0], table2.Columns[0]);

            StringWriter sw1 = new StringWriter();
            StringWriter sw2 = new StringWriter();

            //should not throw an exception
            ds.Tables[0].WriteXmlSchema(sw1);
            ds.Tables[1].WriteXmlSchema(sw2);
        }

        [Fact]
        public void WriteXmlSchema_ForignKeyConstraint()
        {
            DataSet ds1 = new DataSet();

            DataTable table1 = ds1.Tables.Add();
            DataTable table2 = ds1.Tables.Add();

            DataColumn col1_1 = table1.Columns.Add("col1", typeof(int));
            DataColumn col2_1 = table2.Columns.Add("col1", typeof(int));

            table2.Constraints.Add("fk", col1_1, col2_1);

            StringWriter sw1 = new StringWriter();
            ds1.Tables[0].WriteXmlSchema(sw1);
            string xml1 = sw1.ToString();
            Assert.Contains(@"<xs:unique name=""Constraint1"">", xml1, StringComparison.Ordinal);

            StringWriter sw2 = new StringWriter();
            ds1.Tables[1].WriteXmlSchema(sw2);
            string xml2 = sw2.ToString();
            Assert.DoesNotContain(@"<xs:unique name=""Constraint1"">", xml2, StringComparison.Ordinal);
        }

        [Fact]
        public void WriteXmlSchema_Relations_ForeignKeys()
        {
            MemoryStream ms1 = null;
            MemoryStream ms2 = null;
            MemoryStream msA = null;
            MemoryStream msB = null;

            DataSet ds1 = new DataSet();

            DataTable table1 = ds1.Tables.Add("Table 1");
            DataTable table2 = ds1.Tables.Add("Table 2");

            DataColumn col1_1 = table1.Columns.Add("col 1", typeof(int));
            DataColumn col1_2 = table1.Columns.Add("col 2", typeof(int));
            DataColumn col1_3 = table1.Columns.Add("col 3", typeof(int));
            DataColumn col1_4 = table1.Columns.Add("col 4", typeof(int));
            DataColumn col1_5 = table1.Columns.Add("col 5", typeof(int));
            DataColumn col1_6 = table1.Columns.Add("col 6", typeof(int));
            DataColumn col1_7 = table1.Columns.Add("col 7", typeof(int));

            DataColumn col2_1 = table2.Columns.Add("col 1", typeof(int));
            DataColumn col2_2 = table2.Columns.Add("col 2", typeof(int));
            DataColumn col2_3 = table2.Columns.Add("col 3", typeof(int));
            DataColumn col2_4 = table2.Columns.Add("col 4", typeof(int));
            DataColumn col2_5 = table2.Columns.Add("col 5", typeof(int));
            DataColumn col2_6 = table2.Columns.Add("col 6", typeof(int));
            DataColumn col2_7 = table2.Columns.Add("col 7", typeof(int));

            ds1.Relations.Add("rel 1",
                new DataColumn[] { col1_1, col1_2 },
                new DataColumn[] { col2_1, col2_2 },
                false);
            ds1.Relations.Add("rel 2",
                new DataColumn[] { col1_3, col1_4 },
                new DataColumn[] { col2_3, col2_4 },
                true);
            table2.Constraints.Add("fk 1",
                new DataColumn[] { col1_5, col1_6 },
                new DataColumn[] { col2_5, col2_6 });
            table1.Constraints.Add("fk 2",
                new DataColumn[] { col2_5, col2_6 },
                new DataColumn[] { col1_5, col1_6 });

            table1.Constraints.Add("pk 1", col1_7, true);
            table2.Constraints.Add("pk 2", col2_7, true);

            ms1 = new MemoryStream();
            ds1.Tables[0].WriteXmlSchema(ms1);
            ms2 = new MemoryStream();
            ds1.Tables[1].WriteXmlSchema(ms2);

            msA = new MemoryStream(ms1.GetBuffer());
            DataTable dtA = new DataTable();
            dtA.ReadXmlSchema(msA);

            msB = new MemoryStream(ms2.GetBuffer());
            DataTable dtB = new DataTable();
            dtB.ReadXmlSchema(msB);

            Assert.Equal(3, dtA.Constraints.Count);
            Assert.Equal(2, dtB.Constraints.Count);

            Assert.True(dtA.Constraints.Contains("pk 1"));
            Assert.True(dtA.Constraints.Contains("Constraint1"));
            Assert.True(dtA.Constraints.Contains("Constraint2"));
            Assert.True(dtB.Constraints.Contains("pk 2"));
            Assert.True(dtB.Constraints.Contains("Constraint1"));
        }

        [Fact]
        public void WriteXmlSchema_Hierarchy()
        {
            var ds = new DataSet();
            DataTable table1 = new DataTable();
            DataColumn idColumn = table1.Columns.Add("ID", typeof(int));
            table1.Columns.Add("Name", typeof(string));
            table1.PrimaryKey = new DataColumn[] { idColumn };
            DataTable table2 = new DataTable();
            table2.Columns.Add(new DataColumn("OrderID", typeof(int)));
            table2.Columns.Add(new DataColumn("CustomerID", typeof(int)));
            table2.Columns.Add(new DataColumn("OrderDate", typeof(DateTime)));
            table2.PrimaryKey = new DataColumn[] { table2.Columns[0] };
            ds.Tables.Add(table1);
            ds.Tables.Add(table2);
            ds.Relations.Add("CustomerOrder",
                new DataColumn[] { table1.Columns[0] },
                new DataColumn[] { table2.Columns[1] }, true);

            StringWriter writer1 = new StringWriter();
            table1.WriteXmlSchema(writer1, false);
            string expected1 = "<?xml version=\"1.0\" encoding=\"utf-16\"?>\n<xs:schema id=\"NewDataSet\" xmlns=\"\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:msdata=\"urn:schemas-microsoft-com:xml-msdata\">\n  <xs:element name=\"NewDataSet\" msdata:IsDataSet=\"true\" msdata:MainDataTable=\"Table1\" msdata:UseCurrentLocale=\"true\">\n    <xs:complexType>\n      <xs:choice minOccurs=\"0\" maxOccurs=\"unbounded\">\n        <xs:element name=\"Table1\">\n          <xs:complexType>\n            <xs:sequence>\n              <xs:element name=\"ID\" type=\"xs:int\" />\n              <xs:element name=\"Name\" type=\"xs:string\" minOccurs=\"0\" />\n            </xs:sequence>\n          </xs:complexType>\n        </xs:element>\n      </xs:choice>\n    </xs:complexType>\n    <xs:unique name=\"Constraint1\" msdata:PrimaryKey=\"true\">\n      <xs:selector xpath=\".//Table1\" />\n      <xs:field xpath=\"ID\" />\n    </xs:unique>\n  </xs:element>\n</xs:schema>";
            Assert.Equal(expected1, writer1.ToString().Replace("\r\n", "\n"));

            StringWriter writer2 = new StringWriter();
            table1.WriteXmlSchema(writer2, true);
            string expected2 = "<?xml version=\"1.0\" encoding=\"utf-16\"?>\n<xs:schema id=\"NewDataSet\" xmlns=\"\" xmlns:xs=\"http://www.w3.org/2001/XMLSchema\" xmlns:msdata=\"urn:schemas-microsoft-com:xml-msdata\">\n  <xs:element name=\"NewDataSet\" msdata:IsDataSet=\"true\" msdata:MainDataTable=\"Table1\" msdata:UseCurrentLocale=\"true\">\n    <xs:complexType>\n      <xs:choice minOccurs=\"0\" maxOccurs=\"unbounded\">\n        <xs:element name=\"Table1\">\n          <xs:complexType>\n            <xs:sequence>\n              <xs:element name=\"ID\" type=\"xs:int\" />\n              <xs:element name=\"Name\" type=\"xs:string\" minOccurs=\"0\" />\n            </xs:sequence>\n          </xs:complexType>\n        </xs:element>\n        <xs:element name=\"Table2\">\n          <xs:complexType>\n            <xs:sequence>\n              <xs:element name=\"OrderID\" type=\"xs:int\" />\n              <xs:element name=\"CustomerID\" type=\"xs:int\" minOccurs=\"0\" />\n              <xs:element name=\"OrderDate\" type=\"xs:dateTime\" minOccurs=\"0\" />\n            </xs:sequence>\n          </xs:complexType>\n        </xs:element>\n      </xs:choice>\n    </xs:complexType>\n    <xs:unique name=\"Constraint1\" msdata:PrimaryKey=\"true\">\n      <xs:selector xpath=\".//Table1\" />\n      <xs:field xpath=\"ID\" />\n    </xs:unique>\n    <xs:unique name=\"Table2_Constraint1\" msdata:ConstraintName=\"Constraint1\" msdata:PrimaryKey=\"true\">\n      <xs:selector xpath=\".//Table2\" />\n      <xs:field xpath=\"OrderID\" />\n    </xs:unique>\n    <xs:keyref name=\"CustomerOrder\" refer=\"Constraint1\">\n      <xs:selector xpath=\".//Table2\" />\n      <xs:field xpath=\"CustomerID\" />\n    </xs:keyref>\n  </xs:element>\n</xs:schema>";
            Assert.Equal(expected2, writer2.ToString().Replace("\r\n", "\n"));
        }

        [Fact]
        public void ReadWriteXmlSchema_2()
        {
            DataSet ds = new DataSet("dataset");
            ds.Tables.Add("table1");
            ds.Tables.Add("table2");
            ds.Tables[0].Columns.Add("col");
            ds.Tables[1].Columns.Add("col");
            ds.Relations.Add("rel", ds.Tables[0].Columns[0], ds.Tables[1].Columns[0], true);

            MemoryStream ms1 = new MemoryStream();
            ds.Tables[0].WriteXmlSchema(ms1);
            MemoryStream ms2 = new MemoryStream();
            ds.Tables[1].WriteXmlSchema(ms2);

            DataSet ds1 = new DataSet();
            ds1.Tables.Add();
            ds1.Tables.Add();
            ds1.Tables[0].ReadXmlSchema(new MemoryStream(ms1.GetBuffer()));
            ds1.Tables[1].ReadXmlSchema(new MemoryStream(ms2.GetBuffer()));

            Assert.Equal(0, ds1.Relations.Count);
            Assert.Equal(1, ds1.Tables[0].Columns.Count);
            Assert.Equal(1, ds1.Tables[1].Columns.Count);
        }

        [Fact]
        public void ReadWriteXmlSchemaExp_NoRootElmnt()
        {
            MemoryStream ms = new MemoryStream();
            DataTable dtr = new DataTable();

            Assert.Throws<XmlException>(() => dtr.ReadXmlSchema(ms));
        }

        [Fact]
        public void ReadWriteXmlSchemaExp_NoTableName()
        {
            DataTable dtw = new DataTable();
            MemoryStream ms = new MemoryStream();

            Assert.Throws<InvalidOperationException>(() => dtw.WriteXmlSchema(ms));
        }

        [Fact]
        public void ReadWriteXmlSchemaExp_NoFileName()
        {
            DataTable dtw = new DataTable();
            Assert.Throws<ArgumentException>(() => dtw.WriteXmlSchema(string.Empty));
        }

        [Fact]
        public void ReadWriteXmlSchemaExp_TableNameConflict()
        {
            DataTable dtw = new DataTable("Table1");
            StringWriter writer1 = new StringWriter();
            dtw.WriteXmlSchema(writer1);
            DataTable dtr = new DataTable("Table2");
            StringReader reader1 = new StringReader(writer1.ToString());

            Assert.Throws<ArgumentException>(() => dtr.ReadXmlSchema(reader1));
        }

        [Fact]
        public void ReadXmlSchemeWithoutScheme()
        {
            const string xml = @"<CustomElement />";
            using var s = new StringReader(xml);
            DataTable dt = new DataTable();
            dt.ReadXmlSchema(s);
            Assert.Equal("", dt.TableName);
        }

        [Fact]
        public void ReadXmlSchemeWithScheme()
        {
            const string xml = @"<CustomElement>
                  <xs:schema id='NewDataSet' xmlns='' xmlns:xs='http://www.w3.org/2001/XMLSchema' xmlns:msdata='urn:schemas-microsoft-com:xml-msdata'>
                    <xs:element name='NewDataSet' msdata:IsDataSet='true' msdata:MainDataTable='row' msdata:Locale=''>
                      <xs:complexType>
                        <xs:choice minOccurs='0' maxOccurs='unbounded'>
                          <xs:element name='row' msdata:Locale=''>
                            <xs:complexType>
                              <xs:sequence>
                                <xs:element name='Text' type='xs:string' minOccurs='0' />
                              </xs:sequence>
                            </xs:complexType>
                          </xs:element>
                        </xs:choice>
                      </xs:complexType>
                    </xs:element>
                  </xs:schema>
                </CustomElement>";
            using var s = new StringReader(xml);
            DataTable dt = new DataTable();
            dt.ReadXmlSchema(s);
            Assert.Equal("row", dt.TableName);
        }

        [Fact]
        public void ReadXmlSchemeWithBadScheme()
        {
            const string xml = @"<CustomElement>
                  <xs:schema id='NewDataSet' xmlns='' xmlns:xs='http://www.w3.org/2001/BAD' xmlns:msdata='urn:schemas-microsoft-com:xml-msdata'>
                  </xs:schema>
                </CustomElement>";
            AssertExtensions.Throws<ArgumentException>(null, () =>
            {
                using var s = new StringReader(xml);
                DataTable dt = new DataTable();
                dt.ReadXmlSchema(s);
            });
        }

        #endregion // Read/Write XML Tests
    }

    public class MyDataTable : DataTable
    {
        public static int Count;

        public MyDataTable()
        {
            Count++;
        }
    }

    [Serializable]
    public class AppDomainsAndFormatInfo
    {
        [Fact]
        public void Remote()
        {
            int n = (int)Convert.ChangeType("5", typeof(int));
            Assert.Equal(5, n);
        }

        [Fact]
        public void Bug55978()
        {
            DataTable dt = new DataTable();
            dt.Columns.Add("StartDate", typeof(DateTime));

            DataRow dr;
            DateTime now = DateTime.Now;

            // In RowFilter we have a string containing only seconds,
            // but in rows we have a DateTime which also contains milliseconds.
            // The test would fail without this extra minute, when now.Millisecond is 0.
            DateTime rowDate = now.AddMinutes(1);

            for (int i = 0; i < 10; i++)
            {
                dr = dt.NewRow();
                dr["StartDate"] = rowDate.AddDays(i);
                dt.Rows.Add(dr);
            }

            DataView dv = dt.DefaultView;
            dv.RowFilter = string.Format(CultureInfo.InvariantCulture,
                                "StartDate >= #{0}# and StartDate <= #{1}#",
                                now.AddDays(2),
                                now.AddDays(4));
            Assert.Equal(10, dt.Rows.Count);

            int expectedRowCount = 2;
            if (dv.Count != expectedRowCount)
            {
                StringBuilder sb = new();
                sb.AppendLine($"DataView.Rows.Count: Expected: {expectedRowCount}, Actual: {dv.Count}. Debug data: RowFilter: {dv.RowFilter}, date: {now}");
                for (int i = 0; i < dv.Count; i++)
                {
                    sb.Append($"row#{i}: ");
                    foreach (var row in dv[i].Row.ItemArray)
                        sb.Append($"'{row}', ");
                    sb.AppendLine();
                }

                Assert.True(expectedRowCount == dv.Count, sb.ToString());
            }
        }

        [Fact]
        public void Bug82109()
        {
            DataTable tbl = new DataTable();
            tbl.Columns.Add("data", typeof(DateTime));
            DataRow row = tbl.NewRow();
            row["Data"] = new DateTime(2007, 7, 1);
            tbl.Rows.Add(row);

            using (new ThreadCultureChange(CultureInfo.InvariantCulture))
            {
                Select(tbl);
            }

            using (new ThreadCultureChange("it-IT"))
            {
                Select(tbl);
            }

            using (new ThreadCultureChange("fr-FR"))
            {
                Select(tbl);
            }
        }

        private static void Select(DataTable tbl)
        {
            tbl.Locale = CultureInfo.InvariantCulture;
            string filter = string.Format("Data = '{0}'", new DateTime(2007, 7, 1).ToString(CultureInfo.InvariantCulture));
            DataRow[] rows = tbl.Select(filter);
            Assert.Equal(1, rows.Length);
        }
    }
}
