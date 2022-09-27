namespace CovidDataLake.Scripts.Actions;

internal interface IScriptAction
{
    string Name { get; }
    bool Run();
}
