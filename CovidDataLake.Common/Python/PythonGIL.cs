using Python.Runtime;

namespace CovidDataLake.Common.Python
{
    internal class PythonGIL
    {
        public static readonly Py.GILState GILState = Py.GIL();
    }
}
