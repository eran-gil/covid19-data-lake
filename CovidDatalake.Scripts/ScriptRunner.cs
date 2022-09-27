using CovidDataLake.Scripts.Actions;

namespace CovidDataLake.Scripts
{
    internal class ScriptRunner : IScriptRunner
    {
        private readonly Dictionary<string, IScriptAction> _actionsDictionary;

        public ScriptRunner(IEnumerable<IScriptAction> actions)
        {
            _actionsDictionary = actions.ToDictionary(action => action.Name);
        }

        public void Run()
        {
            while (true)
            {
                Console.WriteLine("Please select one of the following actions:");
                var index = 1;
                foreach (var actionDefinition in _actionsDictionary)
                {
                    Console.WriteLine($"{index++}. {actionDefinition.Key}");
                }

                var selectedAction = Console.ReadLine();
                if (!_actionsDictionary.ContainsKey(selectedAction!))
                {
                    Console.WriteLine($"The action {selectedAction} is not supported.");
                    continue;
                }
                var action = _actionsDictionary[selectedAction!];
                action.Run();
            }
            // ReSharper disable once FunctionNeverReturns
        }
    }
}
