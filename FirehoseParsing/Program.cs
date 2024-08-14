internal class Program
{
    private static async Task Main(string[] args)
    {
        //using FileStream stream = File.Open(@"C:\Users\anrem\Downloads\putty_fastdata_knowledgesharing_obj_2batch.json", FileMode.Open); //~70MB
        using FileStream stream = File.Open(@"C:\Users\anrem\Downloads\putty_fastdata_knowledgesharing_obj_1batch_SMALL.json", FileMode.Open); //~70MB
        
        await foreach (var batch in FireHoseJsonParser.ReadBatchesAsync(stream))
        {
            Console.WriteLine($"Batch Position: {batch.Position} Batch size compressed: {batch.RawBatch.Length}");
            File.WriteAllBytes(@$"C:\Users\anrem\Downloads\{Guid.NewGuid()}.deflate", batch.RawBatch.ToArray());
        }
    }
}
