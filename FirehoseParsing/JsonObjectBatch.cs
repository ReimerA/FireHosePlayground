public record JsonObjectBatch
    {
        public ulong Position { get; set; }

        public double Time { get; set; }

        public ReadOnlyMemory<byte> RawBatch { get; set; }
    }