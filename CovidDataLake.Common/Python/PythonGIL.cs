using Python.Runtime;

namespace CovidDataLake.Common.Python
{
    public class PythonGIL
    {
        public static void Initialize()
        {
            PythonEngine.Initialize();
            PythonEngine.BeginAllowThreads();
        }
    }
}
