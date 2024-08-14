using System.Buffers;
using System.IO.Pipelines;
using System.Text.Json;

internal static class FireHoseJsonParser
{

    internal static async IAsyncEnumerable<JsonObjectBatch> ReadBatchesAsync(Stream jsonStream)
    {
        var pipeReader = PipeReader.Create(jsonStream);

        await foreach (var batch in ReadItemsAsync<JsonObjectBatch>(pipeReader))
        {
            yield return batch;
        }

    }

    private static async IAsyncEnumerable<JsonObjectBatch> ReadItemsAsync<T>(PipeReader pipeReader)
    {
            var readerState = GetNewReaderState();
        while (true)
        {
            var result = await pipeReader.ReadAsync();
            var buffer = result.Buffer;
            bool isCompleted = result.IsCompleted;
            SequencePosition bufferPosition = buffer.Start;


            if (buffer.IsEmpty)
            {
                yield break;
            }
            while (true)
            {
                var (value, advanceSequence) = TryReadNextItem(buffer, ref bufferPosition, isCompleted, ref readerState);
                if (value != null)
                {
                    readerState = new JsonReaderState(new JsonReaderOptions()
                    {
                        AllowTrailingCommas = true,
                        CommentHandling = JsonCommentHandling.Skip,
                        //MaxDepth = 1
                    });
                    yield return value;
                }

                if (advanceSequence)
                {
                    pipeReader.AdvanceTo(bufferPosition, buffer.End); //advance our position in the pipe
                    break;
                }
            }

            if (isCompleted)
                yield break;
        }
    }

    private static JsonReaderState GetNewReaderState()
    {
        return new JsonReaderState(new JsonReaderOptions()
        {
            AllowTrailingCommas = true,
            CommentHandling = JsonCommentHandling.Skip,
            //MaxDepth = 1
        });
    }

    static (JsonObjectBatch?, bool) TryReadNextItem(ReadOnlySequence<byte> sequence, ref SequencePosition sequencePosition, bool isCompleted, ref JsonReaderState readerState)
    {
        var reader = new Utf8JsonReader(sequence.Slice(sequencePosition), isCompleted, readerState);
        
        int batchStartDepth = default;
        bool isInBatch = false;


        while (reader.Read())
        {
            //Encoding.UTF8.GetString(reader.ValueSpan).Dump();
            if (isInBatch)
            {
                if (reader.TokenType == JsonTokenType.EndArray && reader.CurrentDepth == batchStartDepth)
                {
                    isInBatch = false;
                }
                //else
                //{
                //	reader.TrySkip();
                //}
                continue;
            }
            switch (reader.TokenType)
            {
                //case JsonTokenType.EndArray:
                //	if (isInBatch && reader.CurrentDepth == batchStartDepth)
                //	{
                //		isInBatch = false;
                //	}
                //	continue;
                case JsonTokenType.PropertyName:
                    {

                        //var test = Encoding.UTF8.GetString(reader.ValueSpan).Dump();
                        if (reader.ValueTextEquals("position"))
                        {
                            reader.TrySkip();
                            var pos = reader.GetInt64();

                            Console.WriteLine($"Position is {pos}.");
                            continue;
                        }

                        if (reader.ValueTextEquals("batch"))
                        {
                            isInBatch = true;
                            batchStartDepth = reader.CurrentDepth;
                            continue;
                        }
                        
                        continue;
                    }
                    case JsonTokenType.EndObject:
                        if (reader.CurrentDepth == 0)
                    {
                        //reader.TrySkip();
                        sequencePosition = reader.Position;
                        readerState = GetNewReaderState();
                        return (new JsonObjectBatch() {}, true);					
                        }
                        continue;
                default:
                    continue;
            }
        }

        sequencePosition = reader.Position;
        readerState = reader.CurrentState;
        return (default, true);
    }

    public record JsonObjectBatch
    {
        public ulong Position { get; set; }

        public double Time { get; set; }

        public ReadOnlyMemory<byte> RawBatch { get; set; }
    }
}