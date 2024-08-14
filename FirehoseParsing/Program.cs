internal class Program
{
    private static async Task Main(string[] args)
    {
        using FileStream stream = File.Open(@"C:\Users\anrem\Downloads\putty_fastdata_knowledgesharing_obj_2batch.json", FileMode.Open); //~70MB

        await foreach (var batch in FireHoseJsonParser.ReadBatchesAsync(stream))
        {
            Console.WriteLine($"Batch Position: {batch.Position}");
        }
    }
}
