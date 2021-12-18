using Python.Runtime;

namespace CovidDataLake.Common.Probabilistic
{
    class PythonCountMinSketch
    {

        // ReSharper disable once NotAccessedField.Local
        private static readonly Py.GILState PyGil;
        private readonly dynamic _sketch;
        private const string CountMinSketchLibrary = "probables";
        private readonly dynamic _pyProbables;

        static PythonCountMinSketch()
        {
            PyGil = Py.GIL();
        }
        public PythonCountMinSketch(float confidence, float errorRate) : this()
        {
            _sketch = _pyProbables.CountMinSketch(confidence: confidence, error_rate: errorRate);
        }

        public PythonCountMinSketch(string filepath) : this()
        {
            _sketch = _pyProbables.CountMinSketch(filepath: filepath);
        }

        private PythonCountMinSketch()
        {
            _pyProbables = Py.Import(CountMinSketchLibrary);
        }

        public void Add(string value)
        {
            _sketch.add(value);
        }

        public int GetOccurrences(string value)
        {
            return _sketch.check(value);
        }

        public void ExportToFile(string filepath)
        {
            _sketch.export(filepath);
        }
    }
}
