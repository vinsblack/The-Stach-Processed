//
// FileTest.cs: Test cases for System.IO.File
//
// Author: 
//     Duncan Mak (duncan@ximian.com)
//     Ville Palo (vi64pa@kolumbus.fi)
//
// (C) 2002 Ximian, Inc. http://www.ximian.com
//
// TODO: Find out why ArgumentOutOfRangeExceptions does not manage to close streams properly
//

using System;
using System.IO;
using System.Globalization;
using System.Threading;
using System.Runtime.InteropServices;


using NUnit.Framework;

namespace MonoTests.System.IO
{
	[TestFixture]
	public class FileTest
	{
		CultureInfo old_culture;
		string tmpFolder;

		[SetUp]
		public void SetUp ()
		{
			tmpFolder = Path.GetTempFileName ();
			if (File.Exists (tmpFolder))
				File.Delete (tmpFolder);
			DeleteDirectory (tmpFolder);
			Directory.CreateDirectory (tmpFolder);
			old_culture = Thread.CurrentThread.CurrentCulture;
			Thread.CurrentThread.CurrentCulture = new CultureInfo ("en-US", false);
		}

		[TearDown]
		public void TearDown ()
		{
			DeleteDirectory (tmpFolder);
			Thread.CurrentThread.CurrentCulture = old_culture;
		}

		string path;
		string testfile;

		[TestFixtureSetUp]
		public void FixtureSetUp ()
		{
			path = Environment.GetFolderPath (Environment.SpecialFolder.Personal);
			testfile = Path.Combine (path, "FileStreamTest.dat");
			File.WriteAllText (testfile, "1");
		}

		[TestFixtureTearDown]
		public void FixtureTearDown ()
		{
			if (File.Exists (testfile))
				File.Delete (testfile);			
		}

		[Test]
		public void TestExists ()
		{
			FileStream s = null;
			string path = tmpFolder + Path.DirectorySeparatorChar + "AFile.txt";
			try {
				Assert.IsFalse (File.Exists (null), "#1");
				Assert.IsFalse (File.Exists (string.Empty), "#2");
				Assert.IsFalse (File.Exists ("  \t\t  \t \n\t\n \n"), "#3");
				DeleteFile (path);
				s = File.Create (path);
				s.Close ();
				Assert.IsTrue (File.Exists (path), "#4");
				Assert.IsFalse (File.Exists (tmpFolder + Path.DirectorySeparatorChar + "doesnotexist"), "#5");
			} finally {
				if (s != null)
					s.Close ();
				DeleteFile (path);
			}
		}

		[Test]
		public void Exists_InvalidFileName () 
		{
			Assert.IsFalse (File.Exists ("><|"), "#1");
			Assert.IsFalse (File.Exists ("?*"), "#2");
		}

		[Test]
		public void Exists_InvalidDirectory () 
		{
			Assert.IsFalse (File.Exists (Path.Combine ("does not exist", "file.txt")));
		}

		[Test]
		public void Create_Path_Null ()
		{
			try {
				File.Create (null);
				Assert.Fail ("#1");
			} catch (ArgumentNullException ex) {
				Assert.AreEqual (typeof (ArgumentNullException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.AreEqual ("path", ex.ParamName, "#5");
			}
		}

		[Test]
		public void Create_Path_Directory ()
		{
			string path = Path.Combine (tmpFolder, "foo");
			Directory.CreateDirectory (path);
			try {
				File.Create (path);
				Assert.Fail ("#1");
			} catch (UnauthorizedAccessException ex) {
				// Access to the path '...' is denied
				Assert.AreEqual (typeof (UnauthorizedAccessException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsTrue (ex.Message.IndexOf (path) != -1, "#5");
			} finally {
				DeleteDirectory (path);
			}
		}

		[Test]
		public void Create_Path_Empty ()
		{
			try {
				File.Create (string.Empty);
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Empty file name is not legal
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		internal const string LIBC = "libc";
		// geteuid(2)
		//    uid_t geteuid(void);
		[DllImport (LIBC, SetLastError=true)]
		static extern uint geteuid ();

		static bool RunningAsRoot // FIXME?
		{
			get {
#if WASM
				return false;
#else
				//return RunningOnUnix && System.Security.WindowsIdentity.GetCurrentToken () == IntPtr.Zero;
				return RunningOnUnix && geteuid () == 0;
#endif
			}
		}

		[Test]
		public void Create_Path_ReadOnly ()
		{
			if (RunningAsRoot) // FIXME?
				Assert.Ignore ("Setting readonly in Mono does not block root writes.");

			string path = Path.Combine (tmpFolder, "foo");
			File.Create (path).Close ();
			File.SetAttributes (path, FileAttributes.ReadOnly);
			try {
				File.Create (path);
				Assert.Fail ("#1");
			} catch (UnauthorizedAccessException ex) {
				// Access to the path '...' is denied
				Assert.AreEqual (typeof (UnauthorizedAccessException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsTrue (ex.Message.IndexOf (path) != -1, "#5");
			} finally {
				File.SetAttributes (path, FileAttributes.Normal);
			}
		}

		[Test]
		public void Create_Path_Whitespace ()
		{
			// FIXME? This is valid on Unix and can work on Windows.
			// This test and Mono have in mind an incorrect low common denominator.
			try {
				File.Create (" ");
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// The path is not of a legal form
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void Create_Directory_DoesNotExist ()
		{
			FileStream stream = null;
			string path = tmpFolder + Path.DirectorySeparatorChar + "directory_does_not_exist" + Path.DirectorySeparatorChar + "foo";
			
			try {
				stream = File.Create (path);
				Assert.Fail ("#1");
			} catch (DirectoryNotFoundException ex) {
				// Could not find a part of the path "..."
				Assert.AreEqual (typeof (DirectoryNotFoundException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsTrue (ex.Message.IndexOf (path) != -1, "#5");
			} finally {
				if (stream != null)
					stream.Close ();
				DeleteFile (path);
			}
		}

		[Test]
		public void Create ()
		{
			FileStream stream = null;
			string path = null;

			/* positive test: create resources/foo */
			path = tmpFolder + Path.DirectorySeparatorChar + "foo";
			try {

				stream = File.Create (path);
				Assert.IsTrue (File.Exists (path), "#1");
				stream.Close ();
			} finally {
				if (stream != null)
					stream.Close ();
				DeleteFile (path);
			}

			stream = null;

			/* positive test: repeat test above again to test for overwriting file */
			path = tmpFolder + Path.DirectorySeparatorChar + "foo";
			try {
				stream = File.Create (path);
				Assert.IsTrue (File.Exists (path), "#2");
				stream.Close ();
			} finally {
				if (stream != null)
					stream.Close ();
				DeleteFile (path);
			}
		}

		[Test]
		public void Copy_SourceFileName_Null ()
		{
			try {
				File.Copy (null, "b");
				Assert.Fail ("#1");
			} catch (ArgumentNullException ex) {
				Assert.AreEqual (typeof (ArgumentNullException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.AreEqual ("sourceFileName", ex.ParamName, "#5");
			}
		}

		[Test]
		public void Copy_DestFileName_Null ()
		{
			try {
				File.Copy ("a", null);
				Assert.Fail ("#1");
			} catch (ArgumentNullException ex) {
				Assert.AreEqual (typeof (ArgumentNullException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.AreEqual ("destFileName", ex.ParamName, "#5");
			}
		}

		[Test]
		public void Copy_SourceFileName_Empty ()
		{
			try {
				File.Copy (string.Empty, "b");
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Empty file name is not legal
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.AreEqual ("sourceFileName", ex.ParamName, "#5");
			}
		}

		[Test]
		public void Copy_DestFileName_Empty ()
		{
			try {
				File.Copy ("a", string.Empty);
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Empty file name is not legal
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.AreEqual ("destFileName", ex.ParamName, "#5");
			}
		}

		[Test]
		public void Copy_SourceFileName_Whitespace ()
		{
			try {
				File.Copy (" ", "b");
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// The path is not of a legal form
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void Copy_DestFileName_Whitespace ()
		{
			try {
				File.Copy ("a", " ");
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// The path is not of a legal form
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void Copy_SourceFileName_DoesNotExist ()
		{
			try {
				File.Copy ("doesnotexist", "b");
				Assert.Fail ("#1");
			} catch (FileNotFoundException ex) {
				Assert.AreEqual (typeof (FileNotFoundException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#4");
				Assert.IsNotNull (ex.Message, "#5");
			}
		}

		[Test]
		public void Copy_DestFileName_AlreadyExists ()
		{
			string source = tmpFolder + Path.DirectorySeparatorChar + "AFile.txt";
			string dest = tmpFolder + Path.DirectorySeparatorChar + "bar";
			DeleteFile (source);
			DeleteFile (dest);
			try {
				File.Create (source).Close ();
				File.Copy (source, dest);
				try {
					File.Copy (source, dest);
					Assert.Fail ("#1");
				} catch (IOException ex) {
					// The file '...' already exists.
					Assert.AreEqual (typeof (IOException), ex.GetType (), "#2");
					Assert.IsNull (ex.InnerException, "#3");
					Assert.IsNotNull (ex.Message, "#4");
					Assert.IsTrue (ex.Message.IndexOf (dest) != -1, "#5");
				}
			} finally {
				DeleteFile (dest);
				DeleteFile (source);
			}
		}

		[Test]
		public void Copy_SourceFileName_DestFileName_Same ()
		{
			string source = tmpFolder + Path.DirectorySeparatorChar + "SameFile.txt";
			DeleteFile (source);
			try {
				// new empty file
				File.Create (source).Close ();
				try {
					File.Copy (source, source, true);
					Assert.Fail ("#1");
				} catch (IOException ex) {
					// process cannot access file ... because it is being used by another process
					Assert.IsNull (ex.InnerException, "#2");
					Assert.IsTrue (ex.Message.IndexOf (source) != -1, "#3");
				}
			} finally {
				DeleteFile (source);
			}
		}

		[Test]
		public void Copy ()
		{
			string path1 = tmpFolder + Path.DirectorySeparatorChar + "bar";
			string path2 = tmpFolder + Path.DirectorySeparatorChar + "AFile.txt";
			/* positive test: copy resources/AFile.txt to resources/bar */
			try {
				DeleteFile (path1);
				DeleteFile (path2);

				File.Create (path2).Close ();
				File.Copy (path2, path1);
				Assert.IsTrue (File.Exists (path2), "#A1");
				Assert.IsTrue (File.Exists (path1), "#A2");

				Assert.IsTrue (File.Exists (path1), "#B1");
				File.Copy (path2, path1, true);
				Assert.IsTrue (File.Exists (path2), "#B2");
				Assert.IsTrue (File.Exists (path1), "#B3");
			} finally {
				DeleteFile (path1);
				DeleteFile (path2);
			}
		}

		[Test]
		public void Delete_Path_Null ()
		{
			try {
				File.Delete (null);
				Assert.Fail ("#1");
			} catch (ArgumentNullException ex) {
				Assert.AreEqual (typeof (ArgumentNullException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.AreEqual ("path", ex.ParamName, "#5");
			}
		}

		[Test]
		public void Delete_Path_Empty ()
		{
			try {
				File.Delete (string.Empty);
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Empty file name is not legal
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void Delete_Path_Whitespace ()
		{
			try {
				File.Delete (" ");
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// The path is not of a legal form
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void Delete_Directory_DoesNotExist ()
		{
			string path = tmpFolder + Path.DirectorySeparatorChar + "directory_does_not_exist" + Path.DirectorySeparatorChar + "foo";
			if (Directory.Exists (path))
				Directory.Delete (path, true);

			try {
				File.Delete (path);
			} catch (DirectoryNotFoundException ex) {
				// Could not find a part of the path "..."
				Assert.AreEqual (typeof (DirectoryNotFoundException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsTrue (ex.Message.IndexOf (path) != -1, "#5");
			}
		}

		[Test]
		public void Delete ()
		{
			string foopath = tmpFolder + Path.DirectorySeparatorChar + "foo";
			DeleteFile (foopath);
			try {
				File.Create (foopath).Close ();
				File.Delete (foopath);
				Assert.IsFalse (File.Exists (foopath));
				File.Delete (foopath);
			} finally {
				DeleteFile (foopath);
			}
		}

		[Test] // bug #323389
		[Category ("NotWorking")]
		public void Delete_FileLock ()
		{
			string path = tmpFolder + Path.DirectorySeparatorChar + "DeleteOpenStreamException";
			DeleteFile (path);
			FileStream stream = null;
			try {
				stream = new FileStream (path, FileMode.OpenOrCreate, FileAccess.ReadWrite);
				try {
					File.Delete (path);
					Assert.Fail ("#1");
				} catch (IOException ex) {
					// The process cannot access the file '...'
					// because it is being used by another process
					Assert.AreEqual (typeof (IOException), ex.GetType (), "#2");
					Assert.IsNull (ex.InnerException, "#3");
					Assert.IsNotNull (ex.Message, "#4");
					Assert.IsTrue (ex.Message.IndexOf (path) != -1, "#5");
				}
			} finally {
				if (stream != null)
					stream.Close ();
				DeleteFile (path);
			}
		}

		[Test]
		[ExpectedException (typeof(UnauthorizedAccessException))]
		public void Delete_File_ReadOnly ()
		{
			if (RunningOnUnix)
				Assert.Ignore ("ReadOnly files can be deleted on unix since fdef50957f508627928c7876a905d5584da45748.");

			string path = tmpFolder + Path.DirectorySeparatorChar + "DeleteReadOnly";
			DeleteFile (path);
			try {
				File.Create (path).Close ();
				File.SetAttributes (path, FileAttributes.ReadOnly);
				File.Delete (path);
			} finally {
				File.SetAttributes (path, FileAttributes.Normal);
				DeleteFile (path);
			}
		}

		[Test]
		public void Delete_NonExisting_NoException ()
		{
			File.Delete (Path.Combine (Directory.GetDirectoryRoot (Directory.GetCurrentDirectory ()), "monononexistingfile.dat"));
		}

		[Test]
		public void GetAttributes_Archive ()
		{
			if (RunningOnUnix)
				Assert.Ignore ("bug #325181: FileAttributes.Archive has no effect on Unix.");

			FileAttributes attrs;

			string path = Path.Combine (tmpFolder, "GetAttributes.tmp");
			File.Create (path).Close ();

			attrs = File.GetAttributes (path);
			Assert.IsTrue ((attrs & FileAttributes.Archive) != 0, "#1");

			attrs &= ~FileAttributes.Archive;
			File.SetAttributes (path, attrs);

			attrs = File.GetAttributes (path);
			Assert.IsFalse ((attrs & FileAttributes.Archive) != 0, "#2");
		}

		[Test]
		public void GetAttributes_Default_File ()
		{
			if (RunningOnUnix)
				Assert.Ignore ("bug #325181: FileAttributes.Archive has no effect on Unix.");

			string path = Path.Combine (tmpFolder, "GetAttributes.tmp");
			File.Create (path).Close ();

			FileAttributes attrs = File.GetAttributes (path);

			Assert.IsTrue ((attrs & FileAttributes.Archive) != 0, "#1");
			Assert.IsFalse ((attrs & FileAttributes.Directory) != 0, "#2");
			Assert.IsFalse ((attrs & FileAttributes.Hidden) != 0, "#3");
			Assert.IsFalse ((attrs & FileAttributes.Normal) != 0, "#4");
			Assert.IsFalse ((attrs & FileAttributes.ReadOnly) != 0, "#5");
			Assert.IsFalse ((attrs & FileAttributes.System) != 0, "#6");
		}

		[Test]
		public void GetAttributes_Default_Directory ()
		{
			FileAttributes attrs = File.GetAttributes (tmpFolder);

			Assert.IsFalse ((attrs & FileAttributes.Archive) != 0, "#1");
			Assert.IsTrue ((attrs & FileAttributes.Directory) != 0, "#2");
			Assert.IsFalse ((attrs & FileAttributes.Hidden) != 0, "#3");
			Assert.IsFalse ((attrs & FileAttributes.Normal) != 0, "#4");
			Assert.IsFalse ((attrs & FileAttributes.ReadOnly) != 0, "#5");
			Assert.IsFalse ((attrs & FileAttributes.System) != 0, "#6");
		}

		[Test]
		public void GetAttributes_Directory ()
		{
			FileAttributes attrs = File.GetAttributes (tmpFolder);

			Assert.IsTrue ((attrs & FileAttributes.Directory) != 0, "#1");

			attrs &= ~FileAttributes.Directory;
			File.SetAttributes (tmpFolder, attrs);

			Assert.IsFalse ((attrs & FileAttributes.Directory) != 0, "#2");

			string path = Path.Combine (tmpFolder, "GetAttributes.tmp");
			File.Create (path).Close ();

			attrs = File.GetAttributes (path);
			attrs |= FileAttributes.Directory;
			File.SetAttributes (path, attrs);

			Assert.IsTrue ((attrs & FileAttributes.Directory) != 0, "#3");
		}

		[Test]
		public void GetAttributes_ReadOnly ()
		{
			if (RunningAsRoot) // FIXME?
				Assert.Ignore ("Setting readonly in Mono does not block root writes.");

			FileAttributes attrs;

			string path = Path.Combine (tmpFolder, "GetAttributes.tmp");
			File.Create (path).Close ();

			attrs = File.GetAttributes (path);
			Assert.IsFalse ((attrs & FileAttributes.ReadOnly) != 0, "#1");

			try {
				attrs |= FileAttributes.ReadOnly;
				File.SetAttributes (path, attrs);

				attrs = File.GetAttributes (path);
				Assert.IsTrue ((attrs & FileAttributes.ReadOnly) != 0, "#2");
			} finally {
				File.SetAttributes (path, FileAttributes.Normal);
			}
		}

		[Test]
		public void GetAttributes_System ()
		{
			if (RunningOnUnix)
				Assert.Ignore ("FileAttributes.System is not supported on Unix.");

			FileAttributes attrs;

			string path = Path.Combine (tmpFolder, "GetAttributes.tmp");
			File.Create (path).Close ();

			attrs = File.GetAttributes (path);
			Assert.IsFalse ((attrs & FileAttributes.System) != 0, "#1");

			attrs |= FileAttributes.System;
			File.SetAttributes (path, FileAttributes.System);

			attrs = File.GetAttributes (path);
			Assert.IsTrue ((attrs & FileAttributes.System) != 0, "#2");
		}

		[Test]
		public void GetAttributes_Path_DoesNotExist ()
		{
			string path = Path.Combine (tmpFolder, "GetAttributes.tmp");
			try {
				File.GetAttributes (path);
				Assert.Fail ("#1");
			} catch (FileNotFoundException ex) {
				Assert.AreEqual (typeof (FileNotFoundException), ex.GetType (), "#2");
				Assert.AreEqual (path, ex.FileName, "#3");
				Assert.IsNull (ex.InnerException, "#4");
				Assert.IsNotNull (ex.Message, "#5");
			}
		}

		[Test]
		public void GetAttributes_Path_Empty ()
		{
			try {
				File.GetAttributes (string.Empty);
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Empty file name is not legal
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void GetAttributes_Path_Null ()
		{
			try {
				File.GetAttributes (null);
				Assert.Fail ("#1");
			} catch (ArgumentNullException ex) {
				Assert.AreEqual (typeof (ArgumentNullException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.AreEqual ("path", ex.ParamName, "#5");
			}
		}

		[Test]
		public void Move_SourceFileName_Null ()
		{
			try {
				File.Move (null, "b");
				Assert.Fail ("#1");
			} catch (ArgumentNullException ex) {
				Assert.AreEqual (typeof (ArgumentNullException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.AreEqual ("sourceFileName", ex.ParamName, "#5");
			}
		}

		[Test]
		public void Move_DestFileName_Null ()
		{
			try {
				File.Move ("a", null);
				Assert.Fail ("#1");
			} catch (ArgumentNullException ex) {
				Assert.AreEqual (typeof (ArgumentNullException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.AreEqual ("destFileName", ex.ParamName, "#5");
			}
		}

		[Test]
		public void Move_SourceFileName_Empty ()
		{
			try {
				File.Move (string.Empty, "b");
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Empty file name is not legal
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.AreEqual ("sourceFileName", ex.ParamName, "#5");
			}
		}

		[Test]
		public void Move_DestFileName_Empty ()
		{
			try {
				File.Move ("a", string.Empty);
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Empty file name is not legal
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.AreEqual ("destFileName", ex.ParamName, "#5");
			}
		}

		[Test]
		public void Move_SourceFileName_Whitespace ()
		{
			try {
				File.Move (" ", "b");
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// The path is not of a legal form
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void Move_DestFileName_Whitespace ()
		{
			try {
				File.Move ("a", " ");
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// The path is not of a legal form
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void Move_SourceFileName_DoesNotExist ()
		{
			string file = tmpFolder + Path.DirectorySeparatorChar + "doesnotexist";
			DeleteFile (file);
			try {
				File.Move (file, "b");
				Assert.Fail ("#1");
			} catch (FileNotFoundException ex) {
				Assert.AreEqual (typeof (FileNotFoundException), ex.GetType (), "#2");
				Assert.AreEqual (file, ex.FileName, "#3");
				Assert.IsNull (ex.InnerException, "#4");
				Assert.IsNotNull (ex.Message, "#5");
			}
		}

		[Test]
		public void Move_DestFileName_DirectoryDoesNotExist ()
		{
			string sourceFile = tmpFolder + Path.DirectorySeparatorChar + "foo";
			string destFile = Path.Combine (Path.Combine (tmpFolder, "doesnotexist"), "b");
			DeleteFile (sourceFile);
			try {
				File.Create (sourceFile).Close ();
				try {
					File.Move (sourceFile, destFile);
					Assert.Fail ("#1");
				} catch (DirectoryNotFoundException ex) {
					// Could not find a part of the path
					Assert.AreEqual (typeof (DirectoryNotFoundException), ex.GetType (), "#2");
					Assert.IsNull (ex.InnerException, "#3");
					Assert.IsNotNull (ex.Message, "#4");
				}
			} finally {
				DeleteFile (sourceFile);
			}
		}

		[Test]
		[Category("NotWasm")]
		public void Move_DestFileName_AlreadyExists ()
		{
			string sourceFile = tmpFolder + Path.DirectorySeparatorChar + "foo";
			string destFile;

			// move to same directory
			File.Create (sourceFile).Close ();
			try {
				File.Move (sourceFile, tmpFolder);
				Assert.Fail ("#A1");
			} catch (IOException ex) {
				// Cannot create a file when that file already exists
				Assert.AreEqual (typeof (IOException), ex.GetType (), "#A2");
				Assert.IsNull (ex.InnerException, "#A3");
				Assert.IsNotNull (ex.Message, "#A4");
				Assert.IsFalse (ex.Message.IndexOf (sourceFile) != -1, "#A5");
			} finally {
				DeleteFile (sourceFile);
			}

			// move to exist file
			File.Create (sourceFile).Close ();
			destFile = tmpFolder + Path.DirectorySeparatorChar + "bar";
			File.Create (destFile).Close ();
			try {
				File.Move (sourceFile, destFile);
				Assert.Fail ("#B1");
			} catch (IOException ex) {
				// Cannot create a file when that file already exists
				Assert.AreEqual (typeof (IOException), ex.GetType (), "#B2");
				Assert.IsNull (ex.InnerException, "#B3");
				Assert.IsNotNull (ex.Message, "#B4");
				Assert.IsFalse (ex.Message.IndexOf (sourceFile) != -1, "#B5");
			} finally {
				DeleteFile (sourceFile);
				DeleteFile (destFile);
			}

			// move to existing directory
			File.Create (sourceFile).Close ();
			destFile = tmpFolder + Path.DirectorySeparatorChar + "bar";
			Directory.CreateDirectory (destFile);
			try {
				File.Move (sourceFile, destFile);
				Assert.Fail ("#C1");
			} catch (IOException ex) {
				// Cannot create a file when that file already exists
				Assert.AreEqual (typeof (IOException), ex.GetType (), "#C2");
				Assert.IsNull (ex.InnerException, "#C3");
				Assert.IsNotNull (ex.Message, "#C4");
				Assert.IsFalse (ex.Message.IndexOf (sourceFile) != -1, "#C5");
			} finally {
				DeleteFile (sourceFile);
				DeleteDirectory (destFile);
			}
		}

		[Test]
		[Category("AndroidSdksNotWorking")]
		public void Move ()
		{
			string bar = tmpFolder + Path.DirectorySeparatorChar + "bar";
			string baz = tmpFolder + Path.DirectorySeparatorChar + "baz";
			if (!File.Exists (bar)) {
				FileStream f = File.Create(bar);
				f.Close();
			}
			
			Assert.IsTrue (File.Exists (bar), "#1");
			File.Move (bar, baz);
			Assert.IsFalse (File.Exists (bar), "#2");
			Assert.IsTrue (File.Exists (baz), "#3");

			// Test moving of directories
			string dir = Path.Combine (tmpFolder, "dir");
			string dir2 = Path.Combine (tmpFolder, "dir2");
			string dir_foo = Path.Combine (dir, "foo");
			string dir2_foo = Path.Combine (dir2, "foo");

			if (Directory.Exists (dir))
				Directory.Delete (dir, true);

			Directory.CreateDirectory (dir);
			Directory.CreateDirectory (dir2);
			File.Create (dir_foo).Close ();
			File.Move (dir_foo, dir2_foo);
			Assert.IsTrue (File.Exists (dir2_foo), "#4");
			
			Directory.Delete (dir, true);
			Directory.Delete (dir2, true);
			DeleteFile (dir_foo);
			DeleteFile (dir2_foo);
		}

		[Test]
		public void Move_FileLock ()
		{
			string sourceFile = Path.GetTempFileName ();
			string destFile = Path.GetTempFileName ();

			// source file locked
			using (File.Open (sourceFile, FileMode.Open, FileAccess.ReadWrite, FileShare.None)) {
				try {
					File.Move (sourceFile, destFile);
					Assert.Fail ("#A1");
				} catch (IOException ex) {
					// The process cannot access the file because
					// it is being used by another process
					Assert.AreEqual (typeof (IOException), ex.GetType (), "#A2");
					Assert.IsNull (ex.InnerException, "#A3");
					Assert.IsNotNull (ex.Message, "#A4");
				}
			}

			// destination file locked
			using (File.Open (destFile, FileMode.Open, FileAccess.ReadWrite, FileShare.None)) {
				try {
					File.Move (sourceFile, destFile);
					Assert.Fail ("#B1");
				} catch (IOException ex) {
					// The process cannot access the file because
					// it is being used by another process
					Assert.AreEqual (typeof (IOException), ex.GetType (), "#B2");
					Assert.IsNull (ex.InnerException, "#B3");
					Assert.IsNotNull (ex.Message, "#B4");
				}
			}
		}

		[Test]
		public void Open ()
		{
			string path = null;
			FileStream stream = null;

			path = tmpFolder + Path.DirectorySeparatorChar + "AFile.txt";
			try {
				if (!File.Exists (path))
					stream = File.Create (path);
				stream.Close ();
				stream = File.Open (path, FileMode.Open);
				stream.Close ();
			} finally {
				if (stream != null)
					stream.Close ();
				DeleteFile (path);
			}

			stream = null;

			if (!File.Exists (path))
				File.Create (path).Close ();
			try {
				stream = File.Open (path, FileMode.Open);
				Assert.IsTrue (stream.CanRead, "#A1");
				Assert.IsTrue (stream.CanSeek, "#A2");
				Assert.IsTrue (stream.CanWrite, "#A3");
				stream.Close ();

				stream = File.Open (path, FileMode.Open, FileAccess.Write);
				Assert.IsFalse (stream.CanRead, "#B1");
				Assert.IsTrue (stream.CanSeek, "#B2");
				Assert.IsTrue (stream.CanWrite, "#B3");
				stream.Close ();

				stream = File.Open (path, FileMode.Open, FileAccess.Read);
				Assert.IsTrue (stream.CanRead, "#C1");
				Assert.IsTrue (stream.CanSeek, "#C2");
				Assert.IsFalse (stream.CanWrite, "#C3");
				stream.Close ();
			} finally {
				if (stream != null)
					stream.Close ();
				DeleteFile (path);
			}

			stream = null;

			/* Exception tests */
			path = tmpFolder + Path.DirectorySeparatorChar + "filedoesnotexist";
			try {
				stream = File.Open (path, FileMode.Open);
				Assert.Fail ("#D1");
			} catch (FileNotFoundException ex) {
				Assert.AreEqual (typeof (FileNotFoundException), ex.GetType (), "#D2");
				Assert.AreEqual (path, ex.FileName, "#D3");
				Assert.IsNull (ex.InnerException, "#D4");
				Assert.IsNotNull (ex.Message, "#D5");
			} finally {
				if (stream != null)
					stream.Close ();
				DeleteFile (path);
			}
		}

		[Test]
		public void Open_CreateNewMode_ReadAccess ()
		{
			string path = tmpFolder + Path.DirectorySeparatorChar + "AFile.txt";
			FileStream stream = null;
			try {
				stream = string fullPath = Path.GetFullPath(Path.Combine(basePath, fileName));
if (!fullPath.StartsWith(Path.GetFullPath(basePath)))
    throw new UnauthorizedAccessException();
File.Open(fullPath, FileMode.CreateNew, FileAccess.Read);
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Combining FileMode: CreateNew with FileAccess: Read is invalid
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			} finally {
				if (stream != null)
					stream.Close ();
				DeleteFile (path);
			}
		}

		[Test]
		public void Open_AppendMode_ReadAccess ()
		{
			string path = tmpFolder + Path.DirectorySeparatorChar + "AFile.txt";
			FileStream s = null;
			if (!File.Exists (path))
				File.Create (path).Close ();
			try {
				s = File.Open (path, FileMode.Append, FileAccess.Read);
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Combining FileMode: Append with FileAccess: Read is invalid
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			} finally {
				if (s != null)
					s.Close ();
				DeleteFile (path);
			}
		}

		[Test]
		public void OpenRead ()
		{
			string path = tmpFolder + Path.DirectorySeparatorChar + "AFile.txt";
			if (!File.Exists (path))
				File.Create (path).Close ();
			FileStream stream = null;
			
			try {
				stream = File.OpenRead (path);
				Assert.IsTrue (stream.CanRead, "#1");
				Assert.IsTrue (stream.CanSeek, "#2");
				Assert.IsFalse (stream.CanWrite, "#3");
			} finally {
				if (stream != null)
					stream.Close ();
				DeleteFile (path);
			}
		}

		[Test]
		public void OpenWrite ()
		{
			string path = tmpFolder + Path.DirectorySeparatorChar + "AFile.txt";
			if (!File.Exists (path))
				File.Create (path).Close ();
			FileStream stream = null;

			try {
				stream = File.OpenWrite (path);
				Assert.IsFalse (stream.CanRead, "#1");
				Assert.IsTrue (stream.CanSeek, "#2");
				Assert.IsTrue (stream.CanWrite, "#3");
				stream.Close ();
			} finally {
				if (stream != null)
					stream.Close ();
				DeleteFile (path);
			}
		}

		[Test]
		public void TestGetCreationTime ()
		{
			string path = tmpFolder + Path.DirectorySeparatorChar + "baz";
			DeleteFile (path);

			try {
				File.Create (path).Close();
				DateTime time = File.GetCreationTime (path);
				Assert.IsTrue ((DateTime.Now - time).TotalSeconds < 10);
			} finally {
				DeleteFile (path);
			}
		}

		[Test]
		public void CreationTime ()
		{
			if (RunningOnUnix)
				Assert.Ignore ("Setting the creation time on Unix is not possible.");

			string path = Path.GetTempFileName ();
			try {
				File.SetCreationTime (path, new DateTime (2002, 4, 6, 4, 6, 4));
				DateTime time = File.GetCreationTime (path);
				Assert.AreEqual (2002, time.Year, "#A1");
				Assert.AreEqual (4, time.Month, "#A2");
				Assert.AreEqual (6, time.Day, "#A3");
				Assert.AreEqual (4, time.Hour, "#A4");
				Assert.AreEqual (4, time.Second, "#A5");

				time = TimeZone.CurrentTimeZone.ToLocalTime (File.GetCreationTimeUtc (path));
				Assert.AreEqual (2002, time.Year, "#B1");
				Assert.AreEqual (4, time.Month, "#B2");
				Assert.AreEqual (6, time.Day, "#B3");
				Assert.AreEqual (4, time.Hour, "#B4");
				Assert.AreEqual (4, time.Second, "#B5");

				File.SetCreationTimeUtc (path, new DateTime (2002, 4, 6, 4, 6, 4));
				time = File.GetCreationTimeUtc (path);
				Assert.AreEqual (2002, time.Year, "#C1");
				Assert.AreEqual (4, time.Month, "#C2");
				Assert.AreEqual (6, time.Day, "#C3");
				Assert.AreEqual (4, time.Hour, "#C4");
				Assert.AreEqual (4, time.Second, "#C5");

				time = TimeZone.CurrentTimeZone.ToUniversalTime (File.GetCreationTime (path));
				Assert.AreEqual (2002, time.Year, "#D1");
				Assert.AreEqual (4, time.Month, "#D2");
				Assert.AreEqual (6, time.Day, "#D3");
				Assert.AreEqual (4, time.Hour, "#D4");
				Assert.AreEqual (4, time.Second, "#D5");
			} finally {
				DeleteFile (path);
			}
		}

		[Test]
		[Category ("NotWasm")]
		public void LastAccessTime ()
		{
			string path = tmpFolder + Path.DirectorySeparatorChar + "lastAccessTime";
			if (File.Exists (path))
				File.Delete (path);
			FileStream stream = null;
			try {
				stream = File.Create (path);
				stream.Close ();

				File.SetLastAccessTime (path, new DateTime (2002, 4, 6, 4, 6, 4));
				DateTime time = File.GetLastAccessTime (path);
				Assert.AreEqual (2002, time.Year, "#A1");
				Assert.AreEqual (4, time.Month, "#A2");
				Assert.AreEqual (6, time.Day, "#A3");
				Assert.AreEqual (4, time.Hour, "#A4");
				Assert.AreEqual (4, time.Second, "#A5");

				time = TimeZone.CurrentTimeZone.ToLocalTime (File.GetLastAccessTimeUtc (path));
				Assert.AreEqual (2002, time.Year, "#B1");
				Assert.AreEqual (4, time.Month, "#B2");
				Assert.AreEqual (6, time.Day, "#B3");
				Assert.AreEqual (4, time.Hour, "#B4");
				Assert.AreEqual (4, time.Second, "#B5");

				File.SetLastAccessTimeUtc (path, new DateTime (2002, 4, 6, 4, 6, 4));
				time = File.GetLastAccessTimeUtc (path);
				Assert.AreEqual (2002, time.Year, "#C1");
				Assert.AreEqual (4, time.Month, "#C2");
				Assert.AreEqual (6, time.Day, "#C3");
				Assert.AreEqual (4, time.Hour, "#C4");
				Assert.AreEqual (4, time.Second, "#C5");

				time = TimeZone.CurrentTimeZone.ToUniversalTime (File.GetLastAccessTime (path));
				Assert.AreEqual (2002, time.Year, "#D1");
				Assert.AreEqual (4, time.Month, "#D2");
				Assert.AreEqual (6, time.Day, "#D3");
				Assert.AreEqual (4, time.Hour, "#D4");
				Assert.AreEqual (4, time.Second, "#D5");
			} finally {
				if (stream != null)
					stream.Close ();
				DeleteFile (path);
			}
		}

		[Test]
		public void LastWriteTime ()
		{
			string path = tmpFolder + Path.DirectorySeparatorChar + "lastWriteTime";
			if (File.Exists (path))
				File.Delete (path);
			FileStream stream = null;
			try {
				stream = File.Create (path);
				stream.Close ();

				File.SetLastWriteTime (path, new DateTime (2002, 4, 6, 4, 6, 4));
				DateTime time = File.GetLastWriteTime (path);
				Assert.AreEqual (2002, time.Year, "#A1");
				Assert.AreEqual (4, time.Month, "#A2");
				Assert.AreEqual (6, time.Day, "#A3");
				Assert.AreEqual (4, time.Hour, "#A4");
				Assert.AreEqual (4, time.Second, "#A5");

				time = TimeZone.CurrentTimeZone.ToLocalTime (File.GetLastWriteTimeUtc (path));
				Assert.AreEqual (2002, time.Year, "#B1");
				Assert.AreEqual (4, time.Month, "#B2");
				Assert.AreEqual (6, time.Day, "#B3");
				Assert.AreEqual (4, time.Hour, "#B4");
				Assert.AreEqual (4, time.Second, "#B5");

				File.SetLastWriteTimeUtc (path, new DateTime (2002, 4, 6, 4, 6, 4));
				time = File.GetLastWriteTimeUtc (path);
				Assert.AreEqual (2002, time.Year, "#C1");
				Assert.AreEqual (4, time.Month, "#C2");
				Assert.AreEqual (6, time.Day, "#C3");
				Assert.AreEqual (4, time.Hour, "#C4");
				Assert.AreEqual (4, time.Second, "#C5");

				time = TimeZone.CurrentTimeZone.ToUniversalTime (File.GetLastWriteTime (path));
				Assert.AreEqual (2002, time.Year, "#D1");
				Assert.AreEqual (4, time.Month, "#D2");
				Assert.AreEqual (6, time.Day, "#D3");
				Assert.AreEqual (4, time.Hour, "#D4");
				Assert.AreEqual (4, time.Second, "#D5");
			} finally {
				if (stream != null)
					stream.Close ();
				DeleteFile (path);
			}
		}

		/* regression test for https://github.com/mono/mono/issues/7646 */
		[Test]
		public void LastWriteTimeSubMs ()
		{
			string path = tmpFolder + Path.DirectorySeparatorChar + "lastWriteTimeSubMs";
			if (File.Exists (path))
				File.Delete (path);
			try {
				var fmt = "HH:mm:ss:fffffff";
				for (var i = 0; i < 200; i++) {
					File.WriteAllText (path, "");
					var untouched = File.GetLastWriteTimeUtc (path);
					var now = DateTime.UtcNow;
					var diff = now - untouched;
					// sanity check
					Assert.IsTrue (diff.TotalSeconds >= 0 && diff.TotalSeconds < 2.0, $"Iteration #{i} failed, diff.TotalSeconds: {diff.TotalSeconds}, untouched: {untouched.ToString (fmt)}, now: {now.ToString (fmt)}");
					File.SetLastWriteTimeUtc (path, now);
					var touched = File.GetLastWriteTimeUtc (path);

					Assert.IsTrue (touched >= untouched, $"Iteration #{i} failed, untouched: {untouched.ToString (fmt)} touched: {touched.ToString (fmt)}");
				}
			} finally {
				DeleteFile (path);
			}
		}

		/* regression test from https://github.com/xamarin/xamarin-macios/issues/3929 */
		[Test]
		public void LastWriteTimeSubMsCopy ()
		{
#if MONOTOUCH
			if (Version.TryParse (Environment.GetEnvironmentVariable ("SIMULATOR_RUNTIME_VERSION"), out Version simulatorVersion) && simulatorVersion.Major < 11 && new DriveInfo("/").DriveFormat == "apfs")
				Assert.Inconclusive ("This test doesn't work on old iOS Simulator versions running on newer macOS with APFS.");
#endif
			string path = tmpFolder + Path.DirectorySeparatorChar + "lastWriteTimeSubMs";
			if (File.Exists (path))
				File.Delete (path);

			string path_copy = tmpFolder + Path.DirectorySeparatorChar + "LastWriteTimeSubMs_copy";
			if (File.Exists (path_copy))
				File.Delete (path_copy);

			try {
				var fmt = "HH:mm:ss:fffffff";
				for (var i = 0; i < 200; i++) {
					File.WriteAllText (path, "");
					var untouched = File.GetLastWriteTimeUtc (path);
					File.Copy (path, path_copy);
					var copy_touched = File.GetLastWriteTimeUtc (path_copy);

					Assert.IsTrue (copy_touched >= untouched, $"Iteration #{i} failed, untouched: {untouched.ToString (fmt)} copy_touched: {copy_touched.ToString (fmt)}");
					File.Delete (path_copy);
				}
			} finally {
				DeleteFile (path);
				DeleteFile (path_copy);
			}
		}

		[Test]
		public void GetCreationTime_Path_Null ()
		{
			try {
				File.GetCreationTime (null as string);
				Assert.Fail ("#1");
			} catch (ArgumentNullException ex) {
				Assert.AreEqual (typeof (ArgumentNullException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.AreEqual ("path", ex.ParamName, "#5");
			}
		}

		[Test]
		public void GetCreationTime_Path_Empty ()
		{
			try {
				File.GetCreationTime (string.Empty);
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Empty file name is not legal
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}
	
		[Test]
		public void GetCreationTime_Path_DoesNotExist ()
		{
			string path = tmpFolder + Path.DirectorySeparatorChar + "GetCreationTimeException3";
			DeleteFile (path);

			DateTime time = File.GetCreationTime (path);
			DateTime expectedTime = (new DateTime (1601, 1, 1)).ToLocalTime ();
			Assert.AreEqual (expectedTime.Year, time.Year, "#1");
			Assert.AreEqual (expectedTime.Month, time.Month, "#2");
			Assert.AreEqual (expectedTime.Day, time.Day, "#3");
			Assert.AreEqual (expectedTime.Hour, time.Hour, "#4");
			Assert.AreEqual (expectedTime.Second, time.Second, "#5");
			Assert.AreEqual (expectedTime.Millisecond, time.Millisecond, "#6");
		}

		[Test]
		public void GetCreationTime_Path_Whitespace ()
		{
			try {
				File.GetCreationTime ("    ");
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// The path is not of a legal form
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void GetCreationTime_Path_InvalidPathChars ()
		{
			try {
				File.GetCreationTime (Path.InvalidPathChars [0].ToString ());
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Illegal characters in path
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void GetCreationTimeUtc_Path_Null ()
		{
			try {
				File.GetCreationTimeUtc (null as string);
				Assert.Fail ("#1");
			} catch (ArgumentNullException ex) {
				Assert.AreEqual (typeof (ArgumentNullException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.AreEqual ("path", ex.ParamName, "#5");
			}
		}

		[Test]
		public void GetCreationTimeUtc_Path_Empty ()
		{
			try {
				File.GetCreationTimeUtc (string.Empty);
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Empty file name is not legal
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}
	
		[Test]
		public void GetCreationTimeUtc_Path_DoesNotExist ()
		{
			string path = tmpFolder + Path.DirectorySeparatorChar + "GetCreationTimeUtcException3";
			DeleteFile (path);

			DateTime time = File.GetCreationTimeUtc (path);
			Assert.AreEqual (1601, time.Year, "#1");
			Assert.AreEqual (1, time.Month, "#2");
			Assert.AreEqual (1, time.Day, "#3");
			Assert.AreEqual (0, time.Hour, "#4");
			Assert.AreEqual (0, time.Second, "#5");
			Assert.AreEqual (0, time.Millisecond, "#6");
		}

		[Test]
		public void GetCreationTimeUtc_Path_Whitespace ()
		{
			try {
				File.GetCreationTimeUtc ("    ");
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// The path is not of a legal form
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void GetCreationTimeUtc_Path_InvalidPathChars ()
		{
			try {
				File.GetCreationTimeUtc (Path.InvalidPathChars [0].ToString ());
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Illegal characters in path
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void GetLastAccessTime_Path_Null ()
		{
			try {
				File.GetLastAccessTime (null as string);
				Assert.Fail ("#1");
			} catch (ArgumentNullException ex) {
				Assert.AreEqual (typeof (ArgumentNullException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.AreEqual ("path", ex.ParamName, "#5");
			}
		}

		[Test]
		public void GetLastAccessTime_Path_Empty ()
		{
			try {
				File.GetLastAccessTime (string.Empty);
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Empty file name is not legal
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}
	
		[Test]
		public void GetLastAccessTime_Path_DoesNotExist ()
		{
			string path = tmpFolder + Path.DirectorySeparatorChar + "GetLastAccessTimeException3";
			DeleteFile (path);

			DateTime time = File.GetLastAccessTime (path);
			DateTime expectedTime = (new DateTime (1601, 1, 1)).ToLocalTime ();
			Assert.AreEqual (expectedTime.Year, time.Year, "#1");
			Assert.AreEqual (expectedTime.Month, time.Month, "#2");
			Assert.AreEqual (expectedTime.Day, time.Day, "#3");
			Assert.AreEqual (expectedTime.Hour, time.Hour, "#4");
			Assert.AreEqual (expectedTime.Second, time.Second, "#5");
			Assert.AreEqual (expectedTime.Millisecond, time.Millisecond, "#6");
		}

		[Test]
		public void GetLastAccessTime_Path_Whitespace ()
		{
			try {
				File.GetLastAccessTime ("    ");
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// The path is not of a legal form
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void GetLastAccessTime_Path_InvalidPathChars ()
		{
			try {
				File.GetLastAccessTime (Path.InvalidPathChars [0].ToString ());
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Illegal characters in path
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void GetLastAccessTimeUtc_Path_Null ()
		{
			try {
				File.GetLastAccessTimeUtc (null as string);
				Assert.Fail ("#1");
			} catch (ArgumentNullException ex) {
				Assert.AreEqual (typeof (ArgumentNullException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.AreEqual ("path", ex.ParamName, "#5");
			}
		}

		[Test]
		public void GetLastAccessTimeUtc_Path_Empty ()
		{
			try {
				File.GetLastAccessTimeUtc (string.Empty);
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Empty file name is not legal
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}
	
		[Test]
		public void GetLastAccessTimeUtc_Path_DoesNotExist ()
		{
			string path = tmpFolder + Path.DirectorySeparatorChar + "GetLastAccessTimeUtcException3";
			DeleteFile (path);

			DateTime time = File.GetLastAccessTimeUtc (path);
			Assert.AreEqual (1601, time.Year, "#1");
			Assert.AreEqual (1, time.Month, "#2");
			Assert.AreEqual (1, time.Day, "#3");
			Assert.AreEqual (0, time.Hour, "#4");
			Assert.AreEqual (0, time.Second, "#5");
			Assert.AreEqual (0, time.Millisecond, "#6");
		}

		[Test]
		public void GetLastAccessTimeUtc_Path_Whitespace ()
		{
			try {
				File.GetLastAccessTimeUtc ("    ");
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// The path is not of a legal form
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void GetLastAccessTimeUtc_Path_InvalidPathChars ()
		{
			try {
				File.GetLastAccessTimeUtc (Path.InvalidPathChars [0].ToString ());
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Illegal characters in path
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void GetLastWriteTime_Path_Null ()
		{
			try {
				File.GetLastWriteTime (null as string);
				Assert.Fail ("#1");
			} catch (ArgumentNullException ex) {
				Assert.AreEqual (typeof (ArgumentNullException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.AreEqual ("path", ex.ParamName, "#5");
			}
		}

		[Test]
		public void GetLastWriteTime_Path_Empty ()
		{
			try {
				File.GetLastWriteTime (string.Empty);
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Empty file name is not legal
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}
	
		[Test]
		public void GetLastWriteTime_Path_DoesNotExist ()
		{
			string path = tmpFolder + Path.DirectorySeparatorChar + "GetLastAccessTimeUtcException3";
			DeleteFile (path);

			DateTime time = File.GetLastWriteTime (path);
			DateTime expectedTime = (new DateTime (1601, 1, 1)).ToLocalTime ();
			Assert.AreEqual (expectedTime.Year, time.Year, "#1");
			Assert.AreEqual (expectedTime.Month, time.Month, "#2");
			Assert.AreEqual (expectedTime.Day, time.Day, "#3");
			Assert.AreEqual (expectedTime.Hour, time.Hour, "#4");
			Assert.AreEqual (expectedTime.Second, time.Second, "#5");
			Assert.AreEqual (expectedTime.Millisecond, time.Millisecond, "#6");
		}

		[Test]
		public void GetLastWriteTime_Path_Whitespace ()
		{
			try {
				File.GetLastWriteTime ("    ");
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// The path is not of a legal form
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void GetLastWriteTime_Path_InvalidPathChars ()
		{
			try {
				File.GetLastWriteTime (Path.InvalidPathChars [0].ToString ());
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Illegal characters in path
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void GetLastWriteTimeUtc_Path_Null ()
		{
			try {
				File.GetLastWriteTimeUtc (null as string);
				Assert.Fail ("#1");
			} catch (ArgumentNullException ex) {
				Assert.AreEqual (typeof (ArgumentNullException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.AreEqual ("path", ex.ParamName, "#5");
			}
		}

		[Test]
		public void GetLastWriteTimeUtc_Path_Empty ()
		{
			try {
				File.GetLastWriteTimeUtc (string.Empty);
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Empty file name is not legal
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}
	
		[Test]
		public void GetLastWriteTimeUtc_Path_DoesNotExist ()
		{
			string path = tmpFolder + Path.DirectorySeparatorChar + "GetLastWriteTimeUtcException3";
			DeleteFile (path);

			DateTime time = File.GetLastWriteTimeUtc (path);
			Assert.AreEqual (1601, time.Year, "#1");
			Assert.AreEqual (1, time.Month, "#2");
			Assert.AreEqual (1, time.Day, "#3");
			Assert.AreEqual (0, time.Hour, "#4");
			Assert.AreEqual (0, time.Second, "#5");
			Assert.AreEqual (0, time.Millisecond, "#6");
		}

		[Test]
		public void GetLastWriteTimeUtc_Path_Whitespace ()
		{
			try {
				File.GetLastWriteTimeUtc ("    ");
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// The path is not of a legal form
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void GetLastWriteTimeUtc_Path_InvalidPathChars ()
		{
			try {
				File.GetLastWriteTimeUtc (Path.InvalidPathChars [0].ToString ());
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Illegal characters in path
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void FileStreamClose ()
		{
			string path = tmpFolder + Path.DirectorySeparatorChar + "FileStreamClose";
			FileStream stream = null;
			try {
				stream = File.Create (path);
				stream.Close ();
				File.Delete (path);
			} finally {
				if (stream != null)
					stream.Close ();
				DeleteFile (path);
			}
		}
		
		// SetCreationTime and SetCreationTimeUtc exceptions

		[Test]
		public void SetCreationTime_Path_Null ()
		{
			try {
				File.SetCreationTime (null as string, new DateTime (2000, 12, 12, 11, 59, 59));
				Assert.Fail ("#1");
			} catch (ArgumentNullException ex) {
				Assert.AreEqual (typeof (ArgumentNullException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.AreEqual ("path", ex.ParamName, "#5");
			}
		}

		[Test]
		public void SetCreationTime_Path_Empty ()
		{
			try {
				File.SetCreationTime (string.Empty, new DateTime (2000, 12, 12, 11, 59, 59));
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Empty file name is not legal
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void SetCreationTime_Path_Whitespace ()
		{
			try {
				File.SetCreationTime ("     ", new DateTime (2000, 12, 12, 11, 59, 59));
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// The path is not of a legal form
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void SetCreationTime_Path_InvalidPathChars ()
		{
			// On Unix there are no invalid path chars.
			if (Path.InvalidPathChars.Length > 1) {
				try {
					File.SetCreationTime (Path.InvalidPathChars [1].ToString (),
						new DateTime (2000, 12, 12, 11, 59, 59));
					Assert.Fail ("#1");
				} catch (ArgumentException ex) {
					// Illegal characters in path
					Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
					Assert.IsNull (ex.InnerException, "#3");
					Assert.IsNotNull (ex.Message, "#4");
					Assert.IsNull (ex.ParamName, "#5");
				}
			}
		}

		[Test]
		public void SetCreationTime_Path_DoesNotExist ()
		{
			string path = tmpFolder + Path.DirectorySeparatorChar + "SetCreationTimeFileNotFoundException1";
			DeleteFile (path);
			
			try {
				File.SetCreationTime (path, new DateTime (2000, 12, 12, 11, 59, 59));
				Assert.Fail ("#1");
			} catch (FileNotFoundException ex) {
				Assert.AreEqual (typeof (FileNotFoundException), ex.GetType (), "#2");
				Assert.AreEqual (path, ex.FileName, "#3");
				Assert.IsNull (ex.InnerException, "#4");
				Assert.IsNotNull (ex.Message, "#5");
			}
		}

//		[Test]
//		[ExpectedException(typeof (ArgumentOutOfRangeException))]
//		public void SetCreationTimeArgumentOutOfRangeException1 ()
//		{
//			string path = tmpFolder + Path.DirectorySeparatorChar + "SetCreationTimeArgumentOutOfRangeException1";
//			FileStream stream = null;
//			DeleteFile (path);
//			try {
//				stream = File.Create (path);
//				stream.Close ();
//				File.SetCreationTime (path, new DateTime (1000, 12, 12, 11, 59, 59));
//			} finally {
//				if (stream != null)
//					stream.Close ();
//				DeleteFile (path);
//			}
//		}

		[Test]
		public void SetCreationTimeUtc_Path_Null ()
		{ 
			try {
				File.SetCreationTimeUtc (null as string, new DateTime (2000, 12, 12, 11, 59, 59));
				Assert.Fail ("#1");
			} catch (ArgumentNullException ex) {
				Assert.AreEqual (typeof (ArgumentNullException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.AreEqual ("path", ex.ParamName, "#5");
			}
		}

		[Test]
		public void SetCreationTimeUtc_Path_Empty ()
		{
			try {
				File.SetCreationTimeUtc (string.Empty, new DateTime (2000, 12, 12, 11, 59, 59));
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Empty file name is not legal
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void SetCreationTimeUtc_Path_Whitespace ()
		{
			try {
				File.SetCreationTimeUtc ("     ", new DateTime (2000, 12, 12, 11, 59, 59));
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// The path is not of a legal form
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void SetCreationTimeUtc_Path_InvalidPathChars ()
		{
			// On Unix there are no invalid path chars.
			if (Path.InvalidPathChars.Length > 1) {
				try {
					File.SetCreationTimeUtc (Path.InvalidPathChars [1].ToString (),
						new DateTime (2000, 12, 12, 11, 59, 59));
					Assert.Fail ("#1");
				} catch (ArgumentException ex) {
					// Illegal characters in path
					Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
					Assert.IsNull (ex.InnerException, "#3");
					Assert.IsNotNull (ex.Message, "#4");
					Assert.IsNull (ex.ParamName, "#5");
				}
			}
		}

		[Test]
		public void SetCreationTimeUtc_Path_DoesNotExist ()
		{
			string path = tmpFolder + Path.DirectorySeparatorChar + "SetCreationTimeUtcFileNotFoundException1";
			DeleteFile (path);

			try {
				File.SetCreationTimeUtc (path, new DateTime (2000, 12, 12, 11, 59, 59));
				Assert.Fail ("#1");
			} catch (FileNotFoundException ex) {
				Assert.AreEqual (typeof (FileNotFoundException), ex.GetType (), "#2");
				Assert.AreEqual (path, ex.FileName, "#3");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
			}
		}

//		[Test]
//		[ExpectedException(typeof (ArgumentOutOfRangeException))]
//		public void SetCreationTimeUtcArgumentOutOfRangeException1 ()
//		{
//			string path = tmpFolder + Path.DirectorySeparatorChar + "SetCreationTimeUtcArgumentOutOfRangeException1";
//			DeleteFile (path);
//			FileStream stream = null;
//			try {
//				stream = File.Create (path);
//				stream.Close ();
//				File.SetCreationTimeUtc (path, new DateTime (1000, 12, 12, 11, 59, 59));
//			} finally {
//				if (stream != null)
//					stream.Close();
//				DeleteFile (path);
//			}
//		}

		// SetLastAccessTime and SetLastAccessTimeUtc exceptions

		[Test]
		public void SetLastAccessTime_Path_Null ()
		{
			try {
				File.SetLastAccessTime (null as string, new DateTime (2000, 12, 12, 11, 59, 59));
				Assert.Fail ("#1");
			} catch (ArgumentNullException ex) {
				Assert.AreEqual (typeof (ArgumentNullException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.AreEqual ("path", ex.ParamName, "#5");
			}
		}

		[Test]
		public void SetLastAccessTime_Path_Empty ()
		{
			try {
				File.SetLastAccessTime (string.Empty, new DateTime (2000, 12, 12, 11, 59, 59));
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Empty file name is not legal
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void SetLastAccessTime_Path_Whitespace ()
		{
			try {
				File.SetLastAccessTime ("     ", new DateTime (2000, 12, 12, 11, 59, 59));
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// The path is not of a legal form
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void SetLastAccessTime_Path_InvalidPathChars ()
		{
			// On Unix there are no invalid path chars.
			if (Path.InvalidPathChars.Length > 1) {
				try {
					File.SetLastAccessTime (Path.InvalidPathChars [1].ToString (),
						new DateTime (2000, 12, 12, 11, 59, 59));
					Assert.Fail ("#1");
				} catch (ArgumentException ex) {
					// Illegal characters in path
					Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
					Assert.IsNull (ex.InnerException, "#3");
					Assert.IsNotNull (ex.Message, "#4");
					Assert.IsNull (ex.ParamName, "#5");
				}
			}
		}

		[Test]
		public void SetLastAccessTime_Path_DoesNotExist ()
		{
			string path = tmpFolder + Path.DirectorySeparatorChar + "SetLastAccessTimeFileNotFoundException1";
			DeleteFile (path);

			try {
				File.SetLastAccessTime (path, new DateTime (2000, 12, 12, 11, 59, 59));
				Assert.Fail ("#1");
			} catch (FileNotFoundException ex) {
				Assert.AreEqual (typeof (FileNotFoundException), ex.GetType (), "#2");
				Assert.AreEqual (path, ex.FileName, "#3");
				Assert.IsNull (ex.InnerException, "#4");
				Assert.IsNotNull (ex.Message, "#5");
			}
		}

//		[Test]
//		[ExpectedException(typeof (ArgumentOutOfRangeException))]
//		public void SetLastAccessTimeArgumentOutOfRangeException1 ()
//		{
//			string path = tmpFolder + Path.DirectorySeparatorChar + "SetLastTimeArgumentOutOfRangeException1";
//			DeleteFile (path);
//			FileStream stream = null;
//			try {
//				stream = File.Create (path);
//				stream.Close ();
//				File.SetLastAccessTime (path, new DateTime (1000, 12, 12, 11, 59, 59));
//			} finally {
//				if (stream != null)
//					stream.Close ();
//				DeleteFile (path);
//			}
//		}

		[Test]
		public void SetLastAccessTimeUtc_Path_Null ()
		{
			try {
				File.SetLastAccessTimeUtc (null as string, new DateTime (2000, 12, 12, 11, 59, 59));
				Assert.Fail ("#1");
			} catch (ArgumentNullException ex) {
				Assert.AreEqual (typeof (ArgumentNullException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.AreEqual ("path", ex.ParamName, "#5");
			}
		}

		[Test]
		public void SetCLastAccessTimeUtc_Path_Empty ()
		{
			try {
				File.SetLastAccessTimeUtc (string.Empty, new DateTime (2000, 12, 12, 11, 59, 59));
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Empty file name is not legal
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void SetLastAccessTimeUtc_Path_Whitespace ()
		{
			try {
				File.SetLastAccessTimeUtc ("     ", new DateTime (2000, 12, 12, 11, 59, 59));
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// The path is not of a legal form
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void SetLastAccessTimeUtc_Path_InvalidPathChars ()
		{
			// On Unix there are no invalid path chars.
			if (Path.InvalidPathChars.Length > 1) {
				try {
					File.SetLastAccessTimeUtc (Path.InvalidPathChars [1].ToString (),
						new DateTime (2000, 12, 12, 11, 59, 59));
					Assert.Fail ("#1");
				} catch (ArgumentException ex) {
					// Illegal characters in path
					Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
					Assert.IsNull (ex.InnerException, "#3");
					Assert.IsNotNull (ex.Message, "#4");
					Assert.IsNull (ex.ParamName, "#5");
				}
			}
		}

		[Test]
		public void SetLastAccessTimeUtc_Path_DoesNotExist ()
		{
			string path = tmpFolder + Path.DirectorySeparatorChar + "SetLastAccessTimeUtcFileNotFoundException1";
			DeleteFile (path);

			try {
				File.SetLastAccessTimeUtc (path, new DateTime (2000, 12, 12, 11, 59, 59));
				Assert.Fail ("#1");
			} catch (FileNotFoundException ex) {
				Assert.AreEqual (typeof (FileNotFoundException), ex.GetType (), "#2");
				Assert.AreEqual (path, ex.FileName, "#3");
				Assert.IsNull (ex.InnerException, "#4");
				Assert.IsNotNull (ex.Message, "#5");
			}
		}

//		[Test]
//		[ExpectedException(typeof (ArgumentOutOfRangeException))]
//		public void SetLastAccessTimeUtcArgumentOutOfRangeException1 ()
//		{
//			string path = tmpFolder + Path.DirectorySeparatorChar + "SetLastAccessTimeUtcArgumentOutOfRangeException1";
//			DeleteFile (path);
//			FileStream stream = null;
//			try {
//				stream = File.Create (path);
//				stream.Close ();
//				File.SetLastAccessTimeUtc (path, new DateTime (1000, 12, 12, 11, 59, 59));
//			} finally {
//				if (stream != null)
//					stream.Close ();
//				DeleteFile (path);
//			}
//		}

		// SetLastWriteTime and SetLastWriteTimeUtc exceptions

		[Test]
		public void SetLastWriteTime_Path_Null ()
		{
			try {
				File.SetLastWriteTime (null as string, new DateTime (2000, 12, 12, 11, 59, 59));
				Assert.Fail ("#1");
			} catch (ArgumentNullException ex) {
				Assert.AreEqual (typeof (ArgumentNullException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.AreEqual ("path", ex.ParamName, "#5");
			}
		}

		[Test]
		public void SetLastWriteTime_Path_Empty ()
		{
			try {
				File.SetLastWriteTime (string.Empty, new DateTime (2000, 12, 12, 11, 59, 59));
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Empty file name is not legal
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void SetLastWriteTime_Path_Whitespace ()
		{
			try {
				File.SetLastWriteTime ("     ", new DateTime (2000, 12, 12, 11, 59, 59));
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// The path is not of a legal form
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void SetLastWriteTime_Path_InvalidPathChars ()
		{
			// On Unix there are no invalid path chars.
			if (Path.InvalidPathChars.Length > 1) {
				try {
					File.SetLastWriteTime (Path.InvalidPathChars [1].ToString (),
						new DateTime (2000, 12, 12, 11, 59, 59));
					Assert.Fail ("#1");
				} catch (ArgumentException ex) {
					// Illegal characters in path
					Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
					Assert.IsNull (ex.InnerException, "#3");
					Assert.IsNotNull (ex.Message, "#4");
					Assert.IsNull (ex.ParamName, "#5");
				}
			}
		}

		[Test]
		public void SetLastWriteTime_Path_DoesNotExist ()
		{
			string path = tmpFolder + Path.DirectorySeparatorChar + "SetLastWriteTimeFileNotFoundException1";
			DeleteFile (path);

			try {
				File.SetLastWriteTime (path, new DateTime (2000, 12, 12, 11, 59, 59));
				Assert.Fail ("#1");
			} catch (FileNotFoundException ex) {
				Assert.AreEqual (typeof (FileNotFoundException), ex.GetType (), "#2");
				Assert.AreEqual (path, ex.FileName, "#3");
				Assert.IsNull (ex.InnerException, "#4");
				Assert.IsNotNull (ex.Message, "#5");
			}
		}

//		[Test]
//		[ExpectedException(typeof (ArgumentOutOfRangeException))]
//		public void SetLastWriteTimeArgumentOutOfRangeException1 ()
//		{
//			string path = tmpFolder + Path.DirectorySeparatorChar + "SetLastWriteTimeArgumentOutOfRangeException1";
//			DeleteFile (path);
//			FileStream stream = null;
//			try {
//				stream = File.Create (path);
//				stream.Close ();
//				File.SetLastWriteTime (path, new DateTime (1000, 12, 12, 11, 59, 59));
//			} finally {
//				if (stream != null)
//					stream.Close ();
//				DeleteFile (path);
//			}
//		}

		[Test]
		public void SetLastWriteTimeUtc_Path_Null ()
		{
			try {
				File.SetLastWriteTimeUtc (null as string, new DateTime (2000, 12, 12, 11, 59, 59));
				Assert.Fail ("#1");
			} catch (ArgumentNullException ex) {
				Assert.AreEqual (typeof (ArgumentNullException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.AreEqual ("path", ex.ParamName, "#5");
			}
		}

		[Test]
		public void SetLastWriteTimeUtc_Path_Empty ()
		{
			try {
				File.SetLastWriteTimeUtc (string.Empty, new DateTime (2000, 12, 12, 11, 59, 59));
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// Empty file name is not legal
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void SetLastWriteTimeUtc_Path_Whitespace ()
		{
			try {
				File.SetLastWriteTimeUtc ("     ", new DateTime (2000, 12, 12, 11, 59, 59));
				Assert.Fail ("#1");
			} catch (ArgumentException ex) {
				// The path is not of a legal form
				Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
				Assert.IsNull (ex.InnerException, "#3");
				Assert.IsNotNull (ex.Message, "#4");
				Assert.IsNull (ex.ParamName, "#5");
			}
		}

		[Test]
		public void SetLastWriteTimeUtc_Path_InvalidPathChars ()
		{
			// On Unix there are no invalid path chars.
			if (Path.InvalidPathChars.Length > 1) {
				try {
					File.SetLastWriteTimeUtc (Path.InvalidPathChars [1].ToString (),
						new DateTime (2000, 12, 12, 11, 59, 59));
					Assert.Fail ("#1");
				} catch (ArgumentException ex) {
					// Illegal characters in path
					Assert.AreEqual (typeof (ArgumentException), ex.GetType (), "#2");
					Assert.IsNull (ex.InnerException, "#3");
					Assert.IsNotNull (ex.Message, "#4");
					Assert.IsNull (ex.ParamName, "#5");
				}
			}
		}

		[Test]
		public void SetLastWriteTimeUtc_Path_DoesNotExist ()
		{
			string path = tmpFolder + Path.DirectorySeparatorChar + "SetLastWriteTimeUtcFileNotFoundException1";
			DeleteFile (path);

			try {
				File.SetLastWriteTimeUtc (path, new DateTime (2000, 12, 12, 11, 59, 59));
				Assert.Fail ("#1");
			} catch (FileNotFoundException ex) {
				Assert.AreEqual (typeof (FileNotFoundException), ex.GetType (), "#2");
				Assert.AreEqual (path, ex.FileName, "#3");
				Assert.IsNull (ex.InnerException, "#4");
				Assert.IsNotNull (ex.Message, "#5");
			}
		}

//		[Test]
//		[ExpectedException(typeof (ArgumentOutOfRangeException))]
//		public void SetLastWriteTimeUtcArgumentOutOfRangeException1 ()
//		{
//			string path = tmpFolder + Path.DirectorySeparatorChar + "SetLastWriteTimeUtcArgumentOutOfRangeException1";
//			DeleteFile (path);
//			FileStream stream = null;
//			try {
//				stream = File.Create (path);
//				stream.Close ();
//				File.SetLastWriteTimeUtc (path, new DateTime (1000, 12, 12, 11, 59, 59));
//			} finally {
//				if (stream != null)
//					stream.Close ();
//				DeleteFile (path);
//			}
//		}

		[Test]
		public void OpenAppend ()
		{
			string fn = Path.GetTempFileName ();
			using (FileStream s = File.Open (fn, FileMode.Append)) {
			}
			DeleteFile (fn);
		}

			void Position (long value)
		{
			using (FileStream fs = File.OpenRead (testfile)) {
				fs.Position = value;
				Assert.AreEqual (value, fs.Position, "Position");
				Assert.AreEqual (1, fs.Length, "Length");
			}
		}
		
		[Test]
		public void Position_Small ()
		{
			Position (Int32.MaxValue);
		}

		[Test]
		[Category ("LargeFileSupport")]
		public void Position_Large ()
		{
			// fails if HAVE_LARGE_FILE_SUPPORT is not enabled in device builds
			Position ((long) Int32.MaxValue + 1);
		}
		
		void Seek (long value)
		{
			using (FileStream fs = File.OpenRead (testfile)) {
				fs.Seek (value, SeekOrigin.Begin);
				Assert.AreEqual (value, fs.Position, "Position");
				Assert.AreEqual (1, fs.Length, "Length");
			}
		}
		
		[Test]
		public void Seek_Small ()
		{
			Seek (Int32.MaxValue);
		}

		[Test]
		[Category ("LargeFileSupport")]
		public void Seek_Large ()
		{
			// fails if HAVE_LARGE_FILE_SUPPORT is not enabled in device builds
			Seek ((long) Int32.MaxValue + 1);
		}
		
		void LockUnlock (long value)
		{
			using (FileStream fs = new FileStream (testfile, FileMode.OpenOrCreate, FileAccess.ReadWrite, FileShare.ReadWrite)) {
				fs.Lock (value - 1, 1);
				fs.Unlock (value - 1, 1);
				
				fs.Lock (0, value);
				fs.Unlock (0, value);
			}
		}
		
		[Test]
		public void Lock_Small ()
		{
			LockUnlock ((long) Int32.MaxValue);
		}

		[Test]
		[Category ("LargeFileSupport")]
		public void Lock_Large ()
		{
			// note: already worked without HAVE_LARGE_FILE_SUPPORT
			LockUnlock ((long) Int32.MaxValue + 1);
		}
	
		[Test]
		public void ReadWriteAllText ()
		{
			// The MSDN docs said something about
			// not including a final new line. it looks
			// like that was not true. I'm not sure what
			// that was talking about
			read_all (string.Empty);
			read_all ("\r");
			read_all ("\n");
			read_all ("\r\n");
			read_all ("a\r");
			read_all ("a\n");
			read_all ("a\r\n");
			read_all ("a\ra");
			read_all ("a\na");
			read_all ("a\r\na");
			read_all ("a");
			read_all ("\r\r");
			read_all ("\n\n");
			read_all ("\r\n\r\n");
		}

		[Test]
		[Category("NotWasm")]
		[Category("AndroidSdksNotWorking")]
		public void ReplaceTest ()
		{
			string tmp = Path.Combine (tmpFolder, "ReplaceTest");
			Directory.CreateDirectory (tmp);
			string origFile = Path.Combine (tmp, "origFile");
			string replaceFile = Path.Combine (tmp, "replaceFile");
			string backupFile = Path.Combine (tmp, "backupFile");

			using (StreamWriter sw = File.CreateText (origFile)) {
				sw.WriteLine ("origFile");
			}
			using (StreamWriter sw = File.CreateText (replaceFile)) {
				sw.WriteLine ("replaceFile");
			}
			using (StreamWriter sw = File.CreateText (backupFile)) {
				sw.WriteLine ("backupFile");
			}

			File.Replace (origFile, replaceFile, backupFile);
			Assert.IsFalse (File.Exists (origFile), "#1");
			using (StreamReader sr = File.OpenText (replaceFile)) {
				string txt = sr.ReadLine ();
				Assert.AreEqual ("origFile", txt, "#2");
			}
			using (StreamReader sr = File.OpenText (backupFile)) {
				string txt = sr.ReadLine ();
				Assert.AreEqual ("replaceFile", txt, "#3");
			}
		}

		static bool RunningOnUnix {
			get {
				int p = (int) Environment.OSVersion.Platform;
				return ((p == 4) || (p == 128) || (p == 6));
			}
		}

		void DeleteFile (string path)
		{
			if (File.Exists (path))
				File.Delete (path);
		}

		void DeleteDirectory (string path)
		{
			if (Directory.Exists (path))
				Directory.Delete (path, true);
		}

		void read_all (string s)
		{
			string f = Path.GetTempFileName ();
			try {
				File.WriteAllText (f, s);
				string r = File.ReadAllText (f);
				Assert.AreEqual (s, r);
			} finally {
				DeleteFile (f);
			}
		}

		void MoveTest (FileAccess acc, FileShare share, bool works)
		{
			// use TEMP so since the default location (right along with the assemblies) 
			// will get access denied when running under some environment (e.g. iOS devices)
			var file = Path.Combine (Path.GetTempPath (), "kk597rfdnllh89");

			File.Delete (file + ".old");
			using (var v = File.Create (file)) { }

			using (var stream = new FileStream(file, FileMode.Open, acc, share, 4096, FileOptions.SequentialScan)) {
				try {
					File.Move(file, file + ".old");
					if (!works)
						Assert.Fail ("Move with ({0}) and  ({1}) did not fail", acc, share);
				} catch (IOException) {
					if (works)
						Assert.Fail ("Move with ({0}) and  ({1}) did fail", acc, share);
				}
			}
		}

		[DllImport ("libc", SetLastError=true)]
		public static extern int symlink (string oldpath, string newpath);

		[Test]
#if MONOTOUCH_TV
		[Ignore ("See bug #59239: https://bugzilla.xamarin.com/show_bug.cgi?id=59239")]
#endif
		[Category ("NotWasm")]
		public void SymLinkStats() {
			if (!RunningOnUnix)
				Assert.Ignore ("Symlink are hard on windows");

			var name1 = Path.GetRandomFileName ();
			var name2 = Path.GetRandomFileName ();

			var path1 = Path.Combine (Path.GetTempPath (), name1);
			var path2 = Path.Combine (Path.GetTempPath (), name2);

			File.Delete (path1);
			File.Delete (path2);

			try {			
				using (var f = File.Create(path1)) {
					Assert.IsNotNull (f, "Path1 must be created for symlink stats");
				}

				if (symlink (path1, path2) != 0)
					Assert.Fail ("symlink #1 failed with errno={0}", Marshal.GetLastWin32Error ());
				
				Assert.IsTrue (File.Exists (path1), "File.Exists must return true for path1 symlink stats");
				Assert.IsTrue (File.Exists (path2), "File.Exists must return true for path2 symlink stats");

				try {					
					File.SetLastWriteTime (path1, DateTime.Now.AddMinutes(-5));
					File.SetCreationTime (path1, DateTime.Now.AddMinutes(-5));
					File.SetLastAccessTime (path1, DateTime.Now.AddMinutes(-5));

					Assert.AreNotEqual (File.GetLastWriteTime(path1), File.GetLastWriteTime (path2), "Path1 and Path2 should not have the same last write times");
					Assert.AreNotEqual (File.GetCreationTime(path1), File.GetCreationTime (path2), "Path1 and Path2 should not have the same creation times");
					Assert.AreNotEqual (File.GetLastAccessTime(path1), File.GetLastAccessTime (path2), "Path1 and Path2 should not have the same last access times");
				}
				catch(IOException e) {
					Assert.Fail ("Symlink stats should allow setting/getting time on file");
				}

				File.Delete (path2);
				Assert.IsTrue (File.Exists(path1), "Original file must still exist for symlink stats");
				Assert.IsFalse (File.Exists(path2), "File.Delete must delete symlink only for symlink stats");

				File.Delete (path1); 
				Assert.IsFalse (File.Exists (path1), "File.Delete must delete symlink stats");

			} finally {
				try {
					File.Delete (path1);
					File.Delete (path2);
				} catch (IOException) {
					//Don't double fault any exception from the tests.
				}
			}
		}

		[Test]
#if MONOTOUCH_TV
		[Ignore ("See bug #59239: https://bugzilla.xamarin.com/show_bug.cgi?id=59239")]
#endif
		[Category ("NotWasm")]
		public void SymLinkLoop ()
		{
			if (!RunningOnUnix)
				Assert.Ignore ("Symlink are hard on windows");

			var name1 = Path.GetRandomFileName ();
			var name2 = Path.GetRandomFileName ();

			var path1 = Path.Combine (Path.GetTempPath (), name1);
			var path2 = Path.Combine (Path.GetTempPath (), name2);

			File.Delete (path1);
			File.Delete (path2);

			try {
				if (symlink (path1, path2) != 0)
					Assert.Fail ("symlink #1 failed with errno={0}", Marshal.GetLastWin32Error ());
				if (symlink (path2, path1) != 0)
					Assert.Fail ("symlink #2 failed with errno={0}", Marshal.GetLastWin32Error ());

				Assert.IsTrue (File.Exists (path1), "File.Exists must return true for path1 symlink loop");
				Assert.IsTrue (File.Exists (path2), "File.Exists must return true for path2 symlink loop");

				try {
					using (var f = File.Open (path1, FileMode.Open, FileAccess.Read)) {
						Assert.Fail ("File.Open must throw for symlink loops");
					}
				} catch (IOException ex) {
					Assert.AreEqual (0x80070781u, (uint)ex.HResult, "Ensure HRESULT is correct");
				}

				File.Delete (path1); //Delete must not throw and must work
				Assert.IsFalse (File.Exists (path1), "File.Delete must delete symlink loops");

			} finally {
				try {
					File.Delete (path1);
					File.Delete (path2);
				} catch (IOException) {
					//Don't double fault any exception from the tests.
				}

			}
		}
	}
}
