<?php

namespace Emartech\Chunkulator\Initiator;


class ChunkCalculator
{
    private $chunkSize;


    public function __construct(int $chunkSize)
    {
        $this->chunkSize = $chunkSize;
    }

    public function calculateChunkCount(int $sourceListCount): int
    {
        return ceil($sourceListCount / $this->chunkSize);
    }

    public function getChunkSize(): int
    {
        return $this->chunkSize;
    }
}
