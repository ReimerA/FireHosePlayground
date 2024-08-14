using System.Buffers;
using System.IO.Compression;
using System.IO.Pipelines;
using System.Security;
using System.Text.Json;
using System.Threading.Channels;

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
        var builder = new JsonObjetBatchBuilder();
        
        while (true)
        {
            var result = await pipeReader.ReadAsync();
            var buffer = result.Buffer;
            bool isCompleted = result.IsCompleted;
            SequencePosition bufferPosition = buffer.Start;
            SequencePosition previousPosition = buffer.Start;


            if (buffer.IsEmpty)
            {
                yield break;
            }
            while (true)
            {
                var (done, advanceSequence) = TryReadNextItem(buffer, ref bufferPosition, isCompleted, ref readerState, builder);
                if (done)
                {
                    await builder.WriteData(buffer.Slice(previousPosition, bufferPosition));
                    previousPosition = bufferPosition;
                    JsonObjectBatch batch = await builder.BuildBatch();
                    builder.Reset();
                    yield return batch;
                }

                if (advanceSequence)
                {
                    await builder.WriteData(buffer.Slice(previousPosition, bufferPosition));
                    previousPosition = bufferPosition;
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

    private static (bool done, bool advanceSequence) TryReadNextItem(ReadOnlySequence<byte> sequence, ref SequencePosition sequencePosition, bool isCompleted, ref JsonReaderState readerState, JsonObjetBatchBuilder builder)
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
                            builder.Position = reader.GetUInt64();
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
                        return (true, true);					
                    }
                        continue;
                default:
                    continue;
            }
        }

        sequencePosition = reader.Position;
        readerState = reader.CurrentState;
        return (false, true);
    }

}

internal class JsonObjetBatchBuilder
{
    MemoryStream memoryStream;
    DeflateStream deflateStream;

    public JsonObjetBatchBuilder()
    {
        Reset();
    }

    public ulong Position { get; set; }

    public async Task WriteData(ReadOnlySequence<byte> data)
    {
        await deflateStream.WriteAsync(data.ToArray());
    }

    public async Task<JsonObjectBatch> BuildBatch()
    {
        await deflateStream.FlushAsync();
        deflateStream.Close();
        deflateStream.Dispose();

        var batch = new JsonObjectBatch
        {
            Position = Position,
            RawBatch = memoryStream.ToArray()
        };
        
        deflateStream.Dispose();
        memoryStream.Dispose();

        return batch;
    }

    public void Reset()
    {
        Position = 0;
        memoryStream = new MemoryStream();
        deflateStream = new DeflateStream(memoryStream, CompressionLevel.Fastest);
    }
}
