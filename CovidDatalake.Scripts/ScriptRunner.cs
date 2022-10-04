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

        public async Task Run()
        {
            while (true)
            {
                Console.WriteLine("Please select one of the following actions (by name):");
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
                await action.Run();
            }
            // ReSharper disable once FunctionNeverReturns
        }
    }
}
