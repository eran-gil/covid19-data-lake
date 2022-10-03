namespace CovidDataLake.Scripts.Actions
{
    internal class ExitAction : IScriptAction
    {
        public string Name => "exit";
        public Task<bool> Run()
        {
            Environment.Exit(0);
            return Task.FromResult(true);
        }
    }
}
