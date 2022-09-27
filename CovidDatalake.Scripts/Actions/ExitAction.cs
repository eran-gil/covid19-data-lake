namespace CovidDataLake.Scripts.Actions
{
    internal class ExitAction : IScriptAction
    {
        public string Name => "exit";
        public bool Run()
        {
            Environment.Exit(0);
            return true;
        }
    }
}
